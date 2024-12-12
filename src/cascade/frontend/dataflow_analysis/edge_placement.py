import networkx as nx

from cascade.frontend.intermediate_representation import Statement
from cascade.frontend.intermediate_representation import Block

class EdgePlacement:

    def __init__(self, G: nx.DiGraph, groups: list[list[Statement]]):
        self.G: nx.DiGraph = G
        self.groups: list[list[Statement]] = groups
        self.H: nx.DiGraph = nx.DiGraph()

    def create_dataflow_graph(self):
        """ Using the groups obtained from the method @group_dataflow_nodes this method
            uses G to check between which groups an edge needs to be placed.
        """
        self.add_block_nodes_to_graph()
        self.add_edges_between_nodes()
        self.remove_redundant_edges()
        return self.H

    def add_block_nodes_to_graph(self):
        for i, g in enumerate(self.groups):
            block: Block = Block(g, i)
            self.H.add_node(block)
            block.set_statement_parents()

    def add_edges_between_nodes(self):
        for (n, v) in self.G.edges:
            n_parent: Block = n.get_parent_block()
            v_parent: Block = v.get_parent_block()
            self.H.add_edge(n_parent, v_parent)

    def remove_redundant_edges(self):
        """ Removes redundant edges in 'stateflow' graph
            - An edge (n, v) is redundant if after the removal of (n, v) there still exists a path from n to v. 
        """
        H: nx.DiGraph = self.H
        edge_list: list[tuple[Block, Block]] = list(H.edges)
        for (n, v) in edge_list:
            H.remove_edge(n, v)
            if not nx.has_path(H, n, v):
                H.add_edge(n, v)
