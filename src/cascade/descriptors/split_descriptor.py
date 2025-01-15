from dataclasses import dataclass

from klara.core import nodes
from cascade.frontend.intermediate_representation import StatementDataflowGraph
from cascade.frontend.dataflow_analysis.dataflow_graph_builder import DataflowGraphBuilder


@dataclass
class SplitDescriptor:

    method_name: str
    method_body: list[nodes.BaseNode]
    live_variables: list
    is_if_condition: bool = False
    dataflow: StatementDataflowGraph = None

    def build_dataflow(self):
        dataflow_graph: StatementDataflowGraph = DataflowGraphBuilder.build(self.method_body, self.live_variables)
        dataflow_graph.set_name(self.method_name)
        self.dataflow = dataflow_graph
    
    def __hash__(self):
        return hash(self.method_name)
