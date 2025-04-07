from dataclasses import dataclass
from typing import Iterable, Optional
import networkx as nx

from cascade.frontend.generator.unparser import unparse
from cascade.frontend.cfg.statement import Statement


@dataclass
class ControlFlowGraph:
    """Control Flow Graph represented as a directed graph.

    Nodes are Statements, and edges are either PO/True/False.
    """
    graph: nx.DiGraph
    instance_type_map: dict[str, str] = None # {"instance_name": "EntityType"}
    method_name: str = None
    _last_node: list[Statement] = None
    _sources: list[Statement] = None

    def __init__(self):
        self.graph = nx.DiGraph()
        self._sources = []
        self._last_node = []

    def set_name(self, name: str):
        self.name = name
    
    def append_statement(self, node: Statement):
        self.graph.add_node(node)

        if len(self._sources) == 0:
            self._sources = [node]

        for ln in self._last_node:
            self.graph.add_edge(ln, node)
        self._last_node = [node]


    def append_subgraph(self, to_node: Statement, subgraph: 'ControlFlowGraph', **edge_attr):
        if subgraph.graph.number_of_nodes == 0:
            return
        for node in subgraph.get_nodes():
            self.graph.add_node(node)
        for edge in subgraph.graph.edges:
            self.graph.add_edge(edge[0], edge[1])
        assert len((s:=subgraph.get_source_nodes())) == 1
        self.graph.add_edge(to_node, s[0], **edge_attr)
        
    def remove_node(self, node: Statement):
        """Remove a node and it's adjacent edges"""
        if node == self.get_single_source():
            succ = list(self.graph.successors(node))
            # assert len(succ) <= 1, "Can't remove node with more than one successor"
            self._sources = succ
        if node == self._last_node:
            raise NotImplementedError("Update last node")
        
        self.graph.remove_node(node)

    def get_single_source(self,) -> Optional[Statement]:
        """Get the source of this CFG. Returns None if there are 0 or 2+ sources."""
        if len(self._sources) == 1:
            return self._sources[0]
        else:
            return None

    def get_single_successor(self, node: Statement) -> Optional[Statement]:
        """Get the successor of this node. Returns None if there are 0 or 2+ successors."""
        succ = list(self.graph.successors(node))
        if len(succ) == 1:
            return succ[0]
        else:
            return None

    def get_nodes(self) -> Iterable[Statement]:
        return self.graph.nodes
    
    def get_edges(self) -> Iterable[tuple[int, int]]:
        return [(u.block_num, v.block_num) for u, v in self.graph.edges]
    
    def get_source_nodes(self) -> list[Statement]:
        return self._sources
    
    def to_dot(self) -> str:    
        dot_string = "digraph CFG {\n"
        
        # Add nodes
        for node in self.get_nodes():
            dot_string += f'    {node.block_num} [label="{unparse(node.block)}"];\n'
        
        # Add edges
        for source, target, type in self.graph.edges.data('type', default='po'):
            dot_string += f'    {source.block_num} -> {target.block_num} [label="{type}"];\n'
        
        dot_string += "}"
        return dot_string
