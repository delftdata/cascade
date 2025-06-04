from klara.core.cfg import  ModuleLabel, TempAssignBlock
from klara.core import nodes

from cascade.frontend.ast_visitors.contains_attribute_visitor import ContainsAttributeVisitor
from cascade.frontend.ast_visitors.variable_getter import VariableGetter
from cascade.frontend.cfg import Statement, ControlFlowGraph


class ControlFlowGraphBuilder:

    def __init__(self, block_list: list, globals: list[str], operators: list[str]):
        self.operators = operators
        self.remote_entities: list = []
        self.block_list: list = block_list
        self.globals = globals

    def make_cfg(self, blocks: list, i = 0) -> tuple[ControlFlowGraph, int]:
        graph = ControlFlowGraph()
        for b in blocks:
            if type(b) in [ModuleLabel, TempAssignBlock]:
                continue
            elif type(b) == nodes.FunctionDef:
                statement = Statement(i, b)
                i += 1
                args = b.args
                function_vars = [f'{a.arg}_0' for a in args.args]

                # detect arguments that are entities e.g. `item1: Item` means item1 is an entity.
                self.remote_entities.extend([f'{a.arg}' for a in args.args if str(a.annotation).strip("'") in self.operators])
 
                statement.extend_targets(function_vars)
                statement.extend_values(function_vars)
                graph.append_statement(statement)
            elif type(b) == nodes.If:
  
                # Make subgraph of both branches
                subgraph_body, i = self.make_cfg(b.body, i)
                subgraph_orelse, i = self.make_cfg(b.orelse, i)
                cond = Statement(i, b.test, is_predicate=True)
                i += 1

                # Add condition & branches to graph
                graph.append_statement(cond)
                graph.append_subgraph(cond, subgraph_body, type=True)
                graph.append_subgraph(cond, subgraph_orelse, type=False)

                if subgraph_orelse.graph.number_of_nodes() == 0:
                    raise NotImplementedError("dataflow structure for if without else is not correct yet")

                # The next node should connect to both subgraph
                graph._last_node = subgraph_body._last_node + subgraph_orelse._last_node
            else:
                statement = Statement(i, b)
                i += 1
                graph.append_statement(statement)
                variable_getter = VariableGetter.get_variable(b)
                targets, values = variable_getter.targets, variable_getter.values
                statement.targets = [t.__repr__() for t in targets]
                statement.values = [v.__repr__() for v in values]
                contains_attribute, attribute = ContainsAttributeVisitor.check_return_attribute(b)
                if contains_attribute:
                    if hasattr(attribute.value, "id"):
                        if attribute.value.id in self.globals:
                            statement.values.remove(attribute.value.id)
                        elif attribute.value.id in self.remote_entities or attribute.value.id in self.operators:
                            statement.set_remote()

                    statement.set_attribute(attribute)
           
        return graph, i
    
    def construct_dataflow_graph(self) -> ControlFlowGraph:
        graph, _ = self.make_cfg(self.block_list)
        return graph
    
    @classmethod
    def build(cls, block_list: list, globals: list[str], operators: list[str]) -> ControlFlowGraph:
        dataflow_graph_builder = cls(block_list, globals, operators)
        return dataflow_graph_builder.construct_dataflow_graph()
