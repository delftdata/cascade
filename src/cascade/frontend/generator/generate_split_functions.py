from itertools import count

import networkx as nx

from cascade.frontend.intermediate_representation import Statement, StatementDataflowGraph
from cascade.frontend.generator.split_function import SplitFunction


from klara.core import nodes
from klara.core.cfg import RawBasicBlock

class GenerateSplittFunctions:

    def __init__(self, dataflow_graph: StatementDataflowGraph, class_name: str, entities: list[str]):
        self.dataflow_graph: StatementDataflowGraph = dataflow_graph
        self.class_name: str = class_name
        self.entities: list[str] = entities
        self.dataflow_node_map = dict()
        self.counter = count()
        self.split_functions = []
    
    def generate_split_functions(self):
        G = self.dataflow_graph.graph
        entry_node: Statement = next(iter(G.nodes))
        assert type(entry_node.block) == nodes.FunctionDef
        # targets = copy.copy(entry_node.targets)
        continuation = list(G.nodes)
        while self.invokes_remote_entity(continuation):
            first_half, continuation = self.split_fuction(G)
            self.add_split_function(first_half)
            G = G.subgraph(continuation)
            # TODO: Add a new source node to continuation
        self.add_split_function(continuation)
    
    def add_split_function(self, statements: list[Statement]): 
        targets, values = set(), set()
        for s in statements:
            targets.update(repr(v) for v in s.targets)
            if s.is_remote():
                for v in s.values:
                    if not self.value_is_entity(v):
                        values.update(repr(v))
                # values.update(repr(v) for v in s.values if not self.value_is_entity(v))
            elif type(s.block) != nodes.FunctionDef:
                values.update(repr(v) for v in s.values if not self.value_is_entity(v))
        i: int = next(self.counter)
        method_name = f'{self.dataflow_graph.name}_{i}'
        split_f: SplitFunction = SplitFunction(i, method_name, statements, targets=targets, values=values, class_name=self.class_name)
        self.split_functions.append(split_f)
    
    def value_is_entity(self, value: nodes.Name) -> bool:
        value_id = value.id
        instance_type_map: dict[str,str] = self.dataflow_graph.instance_type_map
        if not value_id in instance_type_map:
            return False
        entity_type: str = instance_type_map[value_id]
        return entity_type in self.entities

    
    def invokes_remote_entity(self, statments: list[Statement]) -> bool:
        """Returns whether statements contains a remote invocation"""
        return any(s.is_remote() for s in statments)
    
    def split_fuction(self, G: nx.DiGraph):
        """ Produces split functions. Assumes that the runtime will always return to initial function call.
        Therefore functions containing a remote function call (one to a remote entity) will be split into two functions:
        one function adding the keys to the stack of the remote entities to call. And the continuation which the
        function returns to. This way the entity invoking the method does not know anything about 
        - Assumes needs split. i.e. there is a remote entity invoked.
        - Every node on the path to a node included should be included. (because these are the data dependencies)
        - And also the nodes that the nodes listed above are data dependend on.
        - Should also contain a liveness analyses to determine which variables should be passed on to the continuation.
        """
        source: Statement = self.dataflow_graph.get_source_node()
        first_half = set() # A set of nodes that are in the first half of the split function.
        for n in G.nodes:
            n: Statement
            if n == source or not n.is_remote():
                continue
            elif self.no_remote_dependencies_on_path(G, source, n):
                self.add_nodes_path_to_first_half(G, source, n, first_half)
        continuation = set(G.nodes) - first_half # The set of nodes in the continuation.
        return first_half, continuation

    
    def no_remote_dependencies_on_path(self, G: nx.DiGraph, source: Statement, target: Statement) -> bool:
        for path in self.get_all_simple_paths(G, source, target):
            for n in path:
                if n not in [source, target] and n.is_remote():
                    return False
        return True
    
    def add_nodes_path_to_first_half(self, G: nx.DiGraph, source: Statement, statement: Statement, split: set[Statement]):
        for path in self.get_all_simple_paths(G, source, statement):
            for n in path:
                split.add(n)
        
    def get_all_simple_paths(self, G: nx.DiGraph, source: Statement, target: Statement):
        return nx.all_simple_paths(G, source=source, target=target)

    @classmethod
    def generate(cls, dataflow_graph: StatementDataflowGraph, class_name: str, entities: list[str]):
        c = cls(dataflow_graph, class_name, entities)
        c.generate_split_functions()
        return c.split_functions
