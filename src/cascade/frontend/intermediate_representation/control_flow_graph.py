from dataclasses import dataclass
from typing import Iterable
import networkx as nx

from cascade.frontend.generator.unparser import unparse
from cascade.frontend.intermediate_representation.statement import Statement


@dataclass
class ControlFlowGraph:
    """Control Flow Graph represented as a directed graph.

    Nodes are Statements, and edges are either PO/True/False.
    """
    graph: nx.DiGraph
    instance_type_map: dict[str, str] = None # {"instance_name": "EntityType"}
    method_name: str = None
    _last_node: list[Statement] = None
    _source_node: Statement = None

    def __init__(self):
        self.graph = nx.DiGraph()
        self._last_node = []

    def set_name(self, name: str):
        self.name = name
    
    def append_statement(self, node: Statement):
        self.graph.add_node(node)

        if not self._source_node:
            self._source_node = node

        for ln in self._last_node:
            self.graph.add_edge(ln, node)
        self._last_node = [node]


    def append_subgraph(self, to_node: Statement, subgraph: 'ControlFlowGraph', **edge_attr):
        if subgraph.graph.number_of_nodes == 0:
            return
        for node in subgraph.get_nodes():
            self.graph.add_node(node)
        for edge in subgraph.get_edges():
            self.graph.add_edge(edge[0], edge[1])
        assert subgraph._source_node
        self.graph.add_edge(to_node, subgraph._source_node, **edge_attr)
        

    def get_nodes(self) -> Iterable[Statement]:
        return self.graph.nodes
    
    def get_edges(self) -> Iterable[tuple[int, int]]:
        return [(u.block_num, v.block_num) for u, v in self.graph.edges]
    
    def get_source_node(self) -> Statement:
        return self._source_node
    
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
