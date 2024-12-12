from dataclasses import dataclass
import networkx as nx

from cascade.frontend.intermediate_representation.statement_level_dataflow_graph import StatementDataflowGraph

@dataclass
class DataflowGraph:
    """ Similar to statement level dataflow graph only now the nodes are Blocks.
    """
    graph: nx.DiGraph
    color_type_map: dict[int, str] # e.g.: {0: "Item"}
    method_name: str

    def get_nodes(self):
        return self.graph.nodes
