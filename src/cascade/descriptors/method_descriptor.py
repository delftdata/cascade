from klara.core import nodes

from cascade.frontend.dataflow_analysis.dataflow_graph_builder import DataflowGraphBuilder
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
