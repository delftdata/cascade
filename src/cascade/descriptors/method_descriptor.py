from klara.core import nodes

from cascade.frontend.cfg.cfg_builder import ControlFlowGraphBuilder
from cascade.frontend.cfg import ControlFlowGraph


class MethodDescriptor:
    """A descriptor of a class method"""
    
    def __init__(
            self,
            method_name: str,
            method_node: nodes.FunctionDef,
    ):
        self.method_name: str = method_name
        self.method_node: nodes.FunctionDef = method_node
        self.dataflow: ControlFlowGraph = None

    def build_dataflow(self):
        statements = [self.method_node] + self.method_node.body
        dataflow_graph: ControlFlowGraph = ControlFlowGraphBuilder.build(statements)
        dataflow_graph.set_name(self.method_name)
        self.dataflow = dataflow_graph

