from itertools import count 

import networkx as nx


from klara.core import cfg
from klara.core import nodes

from cascade.frontend.intermediate_representation import Statement, StatementDataflowGraph
from cascade.frontend.ast_visitors import ContainsAttributeVisitor, VariableGetter


class DataflowGraphBuilder:

    def __init__(self, block_list: list):
        self.block_list: list = block_list
        self.counter = count()
    
    def next(self):
        return next(self.counter)

    def extract_statment_list(self, block_list):
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
                    statement = self.add_statment_to_graph(b.test, G)
                    self.branch_out(statement, b.body, G)
                    self.branch_out(statement, b.orelse, G)
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
        contains_attribute, attribute = ContainsAttributeVisitor.check_return_attribute(node)
        if contains_attribute:
            if attribute.value.id != 'self':
                # todo: Is only remote if the attribute is an entity. not if the attribute is dataclass0
                statement.set_remote()
        statement.set_attribute(attribute)
        return statement

    def branch_out(self, statement, block_list, G: nx.DiGraph):
        H = self.extract_statment_list(block_list)
        G.add_nodes_from(H)
        G.add_edges_from(H.edges())
        for n in H.nodes:
            G.add_edge(statement, n) 

    
    def construct_dataflow_graph(self, block_list) -> nx.DiGraph:
        G: nx.DiGraph = self.extract_statment_list(block_list)
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
    


