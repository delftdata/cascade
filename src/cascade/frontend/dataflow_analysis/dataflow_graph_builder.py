import networkx as nx


from klara.core.cfg import  ModuleLabel, TempAssignBlock
from klara.core.nodes import Name, FunctionDef

from cascade.frontend.intermediate_representation import Statement, StatementDataflowGraph
from cascade.frontend.ast_visitors import ContainsAttributeVisitor, VariableGetter


class DataflowGraphBuilder:

    def __init__(self, block_list: list):
        self.block_list: list = block_list

    def extract_statment_list(self):
        # TODO: This one should be extended with recursion to handle if/else branches
        statements = []
        i = 0
        for b in self.block_list:
            if type(b) in [ModuleLabel, TempAssignBlock]:
                continue
            elif type(b) == FunctionDef:
                b: FunctionDef
                statement = Statement(i, b)
                i += 1
                args = b.args
                function_vars = [Name.quick_build(f'{a.arg}_0') for a in args.args]
                statement.extend_targets(function_vars)
                statement.extend_values(function_vars)
                statements.append(statement)
            else:
                statement = Statement(i, b)
                i += 1
                statements.append(statement)
                variable_getter = VariableGetter.get_variable(b)
                targets, values = variable_getter.targets, variable_getter.values
                statement.targets = targets
                statement.values = values
                contains_attribute, attribute = ContainsAttributeVisitor.check_return_attribute(b)
                if contains_attribute:
                    if attribute.value.id != 'self':
                        statement.set_remote()

                    statement.set_attribute(attribute)
        return statements
    
    def construct_dataflow_graph(self) -> StatementDataflowGraph:
        statements = self.extract_statment_list()
        G = nx.DiGraph()
        for b1 in statements:
            G.add_node(b1)
            for b2 in statements:
                if b1.block_num != b2.block_num:
                    targets = set(repr(b) for b in b1.targets)
                    values = set(repr(b) for b in b2.values)
                    if targets.intersection(values):
                        G.add_edge(b1, b2)
        return StatementDataflowGraph(G)
    
    @classmethod
    def build(cls, block_list: list) -> StatementDataflowGraph:
        dataflow_graph_builder = cls(block_list)
        return dataflow_graph_builder.construct_dataflow_graph()
