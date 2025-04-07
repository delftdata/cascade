from klara.core.cfg import  ModuleLabel, TempAssignBlock
from klara.core import nodes

from cascade.frontend.cfg import Statement, ControlFlowGraph
from cascade.frontend.ast_visitors import ContainsAttributeVisitor, VariableGetter


class ControlFlowGraphBuilder:

    def __init__(self, block_list: list):
        self.block_list: list = block_list

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
                    if attribute.value.id != 'self':
                        statement.set_remote()

                    statement.set_attribute(attribute)
           
        return graph, i
    
    def construct_dataflow_graph(self) -> ControlFlowGraph:
        graph, _ = self.make_cfg(self.block_list)
        return graph
    
    @classmethod
    def build(cls, block_list: list) -> ControlFlowGraph:
        dataflow_graph_builder = cls(block_list)
        return dataflow_graph_builder.construct_dataflow_graph()
