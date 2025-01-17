from dataclasses import dataclass
import networkx as nx


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
    
    def get_nodes(self):
        return self.graph.nodes
    
    def get_source_node(self):
        return next(iter(self.get_nodes()))
