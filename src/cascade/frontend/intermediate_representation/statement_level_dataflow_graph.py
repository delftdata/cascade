from dataclasses import dataclass
import networkx as nx


@dataclass
class StatementDataflowGraph:
    """ Statement level dataflow graph. Capturs statement level data dependencies in a nx.DiGraph.
        The nodes of the graph are Statements
    """
    graph: nx.DiGraph
    color_type_map: dict[int, str] # e.g.: {0: "Item"}
    method_name: str = None

    def set_name(self, name: str):
        self.name = name
    
    def set_self_color_type(self, entity: str):
        self.color_type_map[1] = entity # sinds 1 is always reserved for self.
