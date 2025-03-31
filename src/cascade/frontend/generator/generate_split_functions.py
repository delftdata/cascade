from itertools import count
from typing import List, Type

import networkx as nx

from cascade.dataflow.dataflow import DataFlow, DataflowRef, Edge
from cascade.dataflow.operator import Block
from cascade.frontend.ast_visitors.extract_type_visitor import ExtractTypeVisitor
from cascade.frontend.dataflow_analysis.dataflow_graph_builder import DataflowGraphBuilder
from cascade.frontend.intermediate_representation import Statement, StatementDataflowGraph
from cascade.frontend.generator.split_function import SplitFunction, SplitFunction2, to_entity_call


from klara.core import nodes

class GenerateSplitFunctions:

    def __init__(self, dataflow_graph: StatementDataflowGraph, class_name: str, entity_map: dict[str, str]):
        self.dataflow_graph: StatementDataflowGraph = dataflow_graph
        self.class_name: str = class_name
        self.entity_map: dict[str, str] = entity_map # {"instance_name": "EntityType"}
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
            first_half, continuation = self.split_function(G)
            self.add_split_function(first_half)
            G = G.subgraph(continuation)
            # TODO: Add a new source node to continuation
        self.add_split_function(continuation)
    
    def add_split_function(self, statements: list[Statement]): 
        targets, values = set(), set()
        for s in statements:
            targets.update(repr(v) for v in s.targets)
            if s.is_remote() or type(s.block) != nodes.FunctionDef:
                values.update(repr(v) for v in s.values if not self.value_is_entity(v))
        i: int = next(self.counter)
        method_name = f'{self.dataflow_graph.name}_{i}'
        split_f: SplitFunction = SplitFunction(i, method_name, statements, targets=targets, values=values, class_name=self.class_name)
        self.split_functions.append(split_f)
    
    def value_is_entity(self, value: nodes.Name) -> bool:
        return value.id in self.entity_map
    
    def invokes_remote_entity(self, statments: list[Statement]) -> bool:
        """Returns whether statements contains a remote invocation"""
        return any(s.is_remote() for s in statments)
    
    def split_function(self, G: nx.DiGraph) -> tuple[list[Statement], list[Statement]]:
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
        first_half = [] # A set of nodes that are in the first half of the split function.
        for n in G.nodes:
            n: Statement
            if n == source or not n.is_remote():
                continue
            elif self.no_remote_dependencies_on_path(G, source, n):
                self.add_nodes_path_to_first_half(G, source, n, first_half)
        fh_set = set(first_half)
        continuation = []
        for node in G.nodes:
            if node not in fh_set:
                continuation.append(node)
        return first_half, continuation

    
    def no_remote_dependencies_on_path(self, G: nx.DiGraph, source: Statement, target: Statement) -> bool:
        print(source, target)
        for path in self.get_all_simple_paths(G, source, target):
            for n in path:
                if n not in [source, target] and n.is_remote():
                    return False
        return True
    
    def add_nodes_path_to_first_half(self, G: nx.DiGraph, source: Statement, statement: Statement, split: list[Statement]):
        for path in self.get_all_simple_paths(G, source, statement):
            for n in path:
                split.append(n)
        
    def get_all_simple_paths(self, G: nx.DiGraph, source: Statement, target: Statement):
        return nx.all_simple_paths(G, source=source, target=target)

    @classmethod
    def generate(cls, dataflow_graph: StatementDataflowGraph, class_name: str, entity_map: dict[str, str]):
        c = cls(dataflow_graph, class_name, entity_map)
        c.generate_split_functions()
        return c.split_functions


class GroupStatements:

    # todo: cfg should be control flow graph, statements should also be a graph
    # list only works for functions with no control flow
    # instead, generate_grouped should take a list of nodes, where each node is a stament,
    # and create a graph of nodes where each node is a list of statments
    # thus statements are grouped if they are all local and in the same block of control flow
    def __init__(self, function_def: nodes.FunctionDef):
        self.function_def = function_def

    def build_cfg(self):
        cfg: StatementDataflowGraph = DataflowGraphBuilder.build([self.function_def] + self.function_def.body)
        self.type_map = ExtractTypeVisitor.extract(self.function_def)
        cfg.name = self.function_def.name

        statements = list(cfg.get_nodes())
        statements.sort(key=lambda s: s.block_num)
        self.statements = statements # TODO: for more complex control flow, use CFG structure instead
        self._grouped_statements: List[List[Statement]] = []
        self.cfg = cfg
    
    def generate_grouped_statements(self) -> List[List[Statement]]:
        entry_node: Statement = self.statements[0]
        assert type(entry_node.block) == nodes.FunctionDef

        grouped_statements = []
        continuation = self.statements[1:]
        while len(continuation) > 0:
            first_half, continuation = self.split_statements(continuation)
            grouped_statements.append(first_half)

        self._grouped_statements = grouped_statements
        return grouped_statements
    
    def split_statements(self, statements: list[Statement]) -> tuple[list[Statement], list[Statement]]:
        """ 
        Split a list of statements, by grouping together statements that are not remote calls.

        As an example, suppose r and s are both statements, where r is a remote call and s is not.

        Here is how the list gets split:
            [r, s, s, r, s] -> [r]       + [s, s, r, s]
            [s, s, r, s, s] -> [s, s]    + [r, s, s]
            [s, s, s]       -> [s, s, s] + []
        """     
        assert len(statements) > 0

        if statements[0].is_remote():
            return [statements[0]], statements[1:]
        
        # find the next remote call
        i = 0
        first_half = []
        while i < len(statements) and not statements[i].is_remote():
            first_half.append(statements[i])
            i += 1

        continuation = statements[i:]
        return first_half, continuation
    
    def build(self, dataflows: dict[DataflowRef, DataFlow], op_name: str) -> tuple[DataFlow, List[Block]]:
        self.build_cfg()

        self.generate_grouped_statements()
        
        blocks = []
        block_num = 0

        args = self.function_def.args
        df = DataFlow("name", "op_name", args)

        last_node = None
        for split in self._grouped_statements:
            print(split)
            if len(split) == 1 and split[0].is_remote():
                # Entity call
                node = to_entity_call(split[0], self.type_map, dataflows)
            else:
                # Group statements together, into a block
                s = SplitFunction2(split, self.cfg.name, block_num, op_name)
                block_num += 1
                node, block = s.to_block()
                blocks.append(block)
          

            if last_node == None:
                last_node = node
                df.add_node(node)
            else:
                df.add_edge(Edge(last_node, node))
                last_node = node

        return df, blocks

