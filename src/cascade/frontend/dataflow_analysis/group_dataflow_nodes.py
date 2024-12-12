import networkx as nx

from cascade.frontend.intermediate_representation import Statement


class GroupDataflowNodes:
    
    def __init__(self, G: nx.DiGraph):
        self.G: nx.DiGraph = G

    def get_statements(self) -> list[Statement]:
        """ Returns a iter with the statements of the program
        """
        return iter(self.G.nodes)

    def get_source_node(self) -> Statement:
        """ In this case the sink node is the node on the last line.
            :returns sink node
        """ 
        return next(self.get_statements())
    
    def group_nodes(self) -> list[list[Statement]]:
        """ Groups nodes
            - First applies breath first coloring.
            - Then groups connected uncollored components
        """
        self.color_source()
        groups: list[list[Statement]] = self.breath_first_coloring()
        groups = self.group_connected_uncollored_components(groups)
        return groups
    
    def color_source(self):
        source: Statement = self.get_source_node()
        source.set_color(-1)
    
    def group_connected_uncollored_components(self, groups):
        """ Takes collored nodes and creates a subgrpah from the uncollored nodes.
            Then transforms this directed subgraph to an undirected graph and groups together 
            the connected components.
        """
        try:
            group_with_color_zero: list[Statement] = next(g for g in groups if g[0].color == 0)
        except StopIteration:
            return groups
        groups.remove(group_with_color_zero)
        H: nx.digraph = self.G.subgraph(group_with_color_zero)
        H_undirected: nx.Graph = H.to_undirected()
        for c in nx.connected_components(H_undirected):
            groups.append(c)
        return groups

    def breath_first_coloring(self) -> list[list[Statement]]:
        """ Groups graph into different subsets based on their color.
            Iterates breath first through dataflow graph and gives an uncollored node (a node with color 0)
            a color if all of its parents have the same color.
        """
        groups: list[list[Statement]] = [] # The new split functions
        source_statement: Statement = self.get_source_node() 
        for (_, statement) in nx.bfs_edges(self.G, source_statement):
            # get statments parents color
            if statement.color != 0:
                continue
            color_set = set()
            for parent in self.G.predecessors(statement):
                color_set.add(parent.color)

            if len(color_set) == 1:
                color = color_set.pop()
                statement.set_color(color)

        group_dict = {}
        for statement in self.get_statements():
            color: int = statement.color
            if color not in group_dict:
                group_dict[color] = []
            group_dict[color].append(statement)
        groups: list[list[Statement]] = list(group_dict.values()) # The new split functions
        return groups 

