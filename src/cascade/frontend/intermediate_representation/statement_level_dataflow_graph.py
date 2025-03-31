from dataclasses import dataclass
from typing import Iterable
import networkx as nx

from cascade.frontend.intermediate_representation.statement import Statement


@dataclass
class StatementDataflowGraph:
    """ Statement level dataflow graph. Capturs statement level data dependencies in a nx.DiGraph.
        The nodes of the graph are Statements
    """
    graph: nx.DiGraph
    instance_type_map: dict[str, str] = None # {"instance_name": "EntityType"}
    method_name: str = None

    def set_name(self, name: str):
        self.name = name
    
    def get_nodes(self) -> Iterable[Statement]:
        return self.graph.nodes
    
    def get_edges(self) -> Iterable[tuple[int, int]]:
        return [(u.block_num, v.block_num) for u, v in self.graph.edges]
    
    def get_source_node(self) -> Statement:
        return next(iter(self.get_nodes()))
