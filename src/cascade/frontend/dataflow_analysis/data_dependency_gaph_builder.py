from itertools import count, chain

import networkx as nx


from klara.core import cfg
from klara.core import nodes

from cascade.frontend.intermediate_representation import Statement, StatementDataflowGraph
from cascade.frontend.ast_visitors import ContainsAttributeVisitor, VariableGetter


class DataDependencyGraphBuilder:


    def extract_data_dependencies(self, block_list):
        G = nx.DiGraph()
        for b in block_list:
            match type(b):
                case cfg.ModuleLabel | cfg.TempAssignBlock:
                    continue
                case nodes.FunctionDef:
                    statement = Statement(self.next(), b)
                    args = b.args
                    function_vars = [nodes.Name.quick_build(f'{a.arg}_0') for a in args.args]
                    statement.extend_targets(function_vars)
                    statement.extend_values(function_vars)
                    G.add_node(statement)
                case nodes.If:
                    self.add_statment_to_graph(b.test, G)
                    self.branch_out(b.body, G)
                    self.branch_out(b.orelse, G)
                case _:
                    self.add_statment_to_graph(b, G)
        return G
    
    def add_statment_to_graph(self, node: nodes.Statement, G: nx.DiGraph) -> Statement:
        statement = Statement(self.next(), node)
        G.add_node(statement)
        variable_getter = VariableGetter.get_variable(node)
        targets, values = variable_getter.targets, variable_getter.values
        statement.targets = targets
        statement.values = values
        contains_attribute, attribute = ContainsAttributeVisitor.check_and_return_attribute(node)
        if contains_attribute:
            if attribute.value.id != 'self':
                # todo: Is only remote if the attribute is an entity. not if the attribute is dataclass0
                statement.set_remote()
        statement.set_attribute(attribute)
        return statement

    def branch_out(self, block_list, G: nx.DiGraph):
        H = self.extract_data_dependencies(block_list)
        G.add_nodes_from(H)
        G.add_edges_from(H.edges())

    def inpsect_phi_function_dependencies(self, funcion_def: nodes.FunctionDef):
        blocks = funcion_def.refer_to_block.blocks
        ssa_code = list(chain.from_iterable(b.ssa_code.code_list for b in blocks))
        ssa_code = ssa_code[-1:] + ssa_code[0:-1]
        return ssa_code
    
    def add_data_dependencies_from_phi_functions(self, block_list, G: nx.DiGraph):
        func_def, *_ = block_list
        ssa_code = self.inpsect_phi_function_dependencies(func_def)
        for block in ssa_code:
            if type(block) == nodes.Assign and block.is_phi:
                self.add_statment_to_graph(block, G)

    def construct_dataflow_graph(self, block_list) -> nx.DiGraph:
        G: nx.DiGraph = self.extract_data_dependencies(block_list)
        self.add_data_dependencies_from_phi_functions(block_list, G)
        self.dataflow_analysis(G)
        return G
    
    def dataflow_analysis(self, G: nx.DiGraph):
        for b1 in G.nodes:
            for b2 in G.nodes:
                if b1.block_num != b2.block_num:
                    targets = set(repr(b) for b in b1.targets)
                    values = set(repr(b) for b in b2.values)
                    if targets.intersection(values):
                        G.add_edge(b1, b2)
        return G
    
    @classmethod
    def build(cls, block_list: list) -> StatementDataflowGraph:
        dataflow_graph_builder = cls(block_list)
        G = dataflow_graph_builder.construct_dataflow_graph(block_list)
        return StatementDataflowGraph(G)
