from klara.core import nodes

from cascade.frontend.dataflow_analysis.dataflow_graph_builder import DataflowGraphBuilder
from cascade.frontend.ast_visitors import ExtractTypeVisitor
from cascade.frontend.dataflow_analysis.dataflow_graph_build_context import DataflowGraphBuildContext
from cascade.frontend.intermediate_representation import StatementDataflowGraph


class MethodDescriptor:
    """A descriptor of a class method"""
    
    def __init__(
            self,
            method_name: str,
            method_node: nodes.FunctionDef,
    ):
        self.method_name: str = method_name
        self.method_node: nodes.FunctionDef = method_node
        self.dataflow: StatementDataflowGraph = None

    def build_dataflow(self):
        statements = [self.method_node] + self.method_node.body
        type_map: dict[str, str] = ExtractTypeVisitor.extract(self.method_node)
        dataflow_graph: StatementDataflowGraph = DataflowGraphBuilder.build(statements, DataflowGraphBuildContext(type_map))
        dataflow_graph.set_name(self.method_name)
        self.dataflow = dataflow_graph

