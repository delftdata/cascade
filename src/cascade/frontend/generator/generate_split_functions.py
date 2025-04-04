from itertools import count
from typing import List, Type

import networkx as nx

from cascade.dataflow.dataflow import DataFlow, DataflowRef, Edge, IfNode, Node
from cascade.frontend.ast_visitors.extract_type_visitor import ExtractTypeVisitor
from cascade.frontend.dataflow_analysis.dataflow_graph_builder import ControlFlowGraphBuilder
from cascade.frontend.intermediate_representation import Statement, ControlFlowGraph
from cascade.frontend.generator.split_function import SplitFunction, LocalBlock, to_entity_call


from klara.core import nodes

class GenerateSplitFunctions:

    def __init__(self, dataflow_graph: ControlFlowGraph, class_name: str, entity_map: dict[str, str]):
        self.dataflow_graph: ControlFlowGraph = dataflow_graph
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
    def generate(cls, dataflow_graph: ControlFlowGraph, class_name: str, entity_map: dict[str, str]):
        c = cls(dataflow_graph, class_name, entity_map)
        c.generate_split_functions()
        return c.split_functions


def split_cfg(blocked_statement_graph: nx.DiGraph) -> nx.DiGraph:
    pass

def blocked_cfg(statement_graph: nx.DiGraph, entry: Statement) -> nx.DiGraph:
    """Transform a cfg (digraph of Statements) into a blocked version, i.e. a
    digraph of tuple(Statements). This pass blocks together the body and orelse
    branches of if blocks, grouping them together.
    This pass treats remote calls as any other statement.

    For example, take the cfg of the following program:

    ```
    a = 10
    b = 20
    if x:
        c = 30
        d = 20
    else:
        e = 10
    f = 10
    ```

    it will get split into the following blocks:

    ```
    block 1:
        a = 10
        b = 20
        if x:
    
    block 2: 
        c = 30
        d = 20
    
    block 3:
        e = 10
    
    block 4:
        f = 10
    ```
    """

    
    grouped_statements = [entry]
    
    succ = list(statement_graph.successors(entry))
    while len(succ) == 1:
        if len(list(statement_graph.predecessors(succ[0]))) > 1:
            break
        grouped_statements.append(succ[0])
        succ = list(statement_graph.successors(succ[0]))
        

    graph = nx.DiGraph()
    
    if len(succ) == 0 or len(succ) == 1:
        last_node = tuple(grouped_statements)
        graph.add_node(last_node)
        return graph
    if len(succ) == 2:
        if len(grouped_statements) > 1:
            before_if, last_node = tuple(grouped_statements[:-1]), tuple([grouped_statements[-1]])
            graph.add_edge(before_if, last_node)
        else:
            last_node = tuple(grouped_statements)
            graph.add_node(last_node)
        # TODO: check that then corresponds to "true" path
        first_then, first_orelse = succ
        then_blocked_graph = blocked_cfg(statement_graph, first_then)
        orelse_blocked_graph = blocked_cfg(statement_graph, first_orelse)
        last_then = list(then_blocked_graph.nodes)[-1]
        last_orelse = list(orelse_blocked_graph.nodes)[-1]

        # check the first node after completed
        succ_then = list(statement_graph.successors(last_then[-1]))
        succ_orelse = list(statement_graph.successors(last_orelse[-1]))
        assert len(succ_then) == 1
        assert len(succ_orelse) == 1
        assert succ_orelse[0] == succ_then[0]

        first_finally = succ_orelse[0]
        finally_graph = blocked_cfg(statement_graph, first_finally)
        
        graph.add_edges_from(then_blocked_graph.edges())
        graph.add_edges_from(orelse_blocked_graph.edges())
        graph.add_edges_from(finally_graph.edges())

        
        first_then = list(then_blocked_graph.nodes)[0]
        first_orelse = list(orelse_blocked_graph.nodes)[0]
        first_finally = list(finally_graph.nodes)[0]

        graph.add_edge(last_node, first_then)
        graph.add_edge(last_node, first_orelse)
        graph.add_edge(last_then, first_finally)
        graph.add_edge(last_orelse, first_finally)
        return graph
    else:
        raise ValueError(succ)



class GroupStatements:

    # todo: cfg should be control flow graph, statements should also be a graph
    # list only works for functions with no control flow
    # instead, generate_grouped should take a list of nodes, where each node is a stament,
    # and create a graph of nodes where each node is a list of statments
    # thus statements are grouped if they are all local and in the same block of control flow
    def __init__(self, function_def: nodes.FunctionDef):
        self.function_def = function_def

    def build_cfg(self):
        cfg: ControlFlowGraph = ControlFlowGraphBuilder.build([self.function_def] + self.function_def.body)
        self.type_map = ExtractTypeVisitor.extract(self.function_def)
        cfg.name = self.function_def.name
        for n in cfg.get_nodes():
            print(n)
        # statements = list(cfg.get_nodes())
        # statements.sort(key=lambda s: s.block_num)
        # self.statements = statements # TODO: for more complex control flow, use CFG structure instead
        self._grouped_statements: List[List[Statement]] = []
        self.cfg = cfg
        self.blocked_cfg = blocked_cfg(cfg.graph, cfg.get_single_source())
    
    def build_df(self, dataflows: dict[DataflowRef, DataFlow], op_name: str) -> DataFlow:
        entry_node: Statement = self.cfg.get_source_nodes()[0]
        assert type(entry_node.block) == nodes.FunctionDef
        self.cfg.remove_node(entry_node)

        df_ref = DataflowRef(op_name, self.cfg.name)
        df = dataflows[df_ref]

        node_id_map = {}

        block_num = 0
        for statement_block in self.blocked_cfg.nodes:
            if len(statement_block) == 1 and statement_block[0].is_remote():
                node = to_entity_call(statement_block[0], self.type_map, dataflows)
            elif len(statement_block) == 1 and statement_block[0].is_predicate:
                node = IfNode()
            else:
                block = LocalBlock(list(statement_block), self.cfg.name, block_num, op_name)
                block_num += 1
                node = block.to_node()
                df.add_block(block)
            node_id_map[statement_block] = node.id
            df.add_node(node)

        for source, target, if_result in self.blocked_cfg.edges.data('type', default=None):
            source_id = node_id_map[source]
            target_id = node_id_map[target]
            df.add_edge_refs(source_id, target_id, if_result)

        return df
    
    def build_df_old(self, dataflows: dict[DataflowRef, DataFlow], op_name: str) -> DataFlow:
        entry_node: Statement = self.cfg.get_source_nodes()[0]
        assert type(entry_node.block) == nodes.FunctionDef

        df_ref = DataflowRef(op_name, self.cfg.name)
        df = dataflows[df_ref]

        last_node = None
        block_num = 0

        while len(self.cfg.graph) > 0:
            print(df.to_dot())

            source = self.cfg.get_single_source()
            if source is not None and source.is_predicate:
                node = IfNode()
                self.cfg.remove_node(source)
            else:
                group = self.split_graph(self.cfg)

                if len(group) == 1 and group[0].is_remote():
                    # Entity call
                    node = to_entity_call(group[0], self.type_map, dataflows)
                else:
                    # Group statements together, into a block
                    block = LocalBlock(group, self.cfg.name, block_num, op_name)
                    block_num += 1
                    node = block.to_node()
                    print(block.to_string())
                    df.blocks[block.get_method_name()] = block

            if last_node == None:
                last_node = node
                df.add_node(node)
                df.entry = [node]
            else:
                df.add_edge(Edge(last_node, node))
                last_node = node

        return df
    
    def split_graph(self, graph: ControlFlowGraph) -> list[Statement]:
        if len(graph.graph) == 0:
            return []

        source = graph.get_source_nodes()[0]
        if source.is_remote():
            graph.remove_node(source)
            return [source]
        
        # find the next remote call
        local_group = [source]
        node = graph.get_single_successor(source)
        graph.remove_node(source)

        while node is not None and not node.is_remote() and not node.is_predicate:
            if len(list(graph.graph.predecessors(node))) > 1:
                break
            local_group.append(node)
            succ = graph.get_single_successor(node)
            graph.remove_node(node)
            node = succ

        return local_group
    
    """ 
        Split a list of statements, by grouping together statements that are not remote calls.
        The graph becomes a subgraph, with the statments removed.

        As an example, suppose r and s are both statements, where r is a remote call and s is not.

        Here is how the list gets split:
            [r, s, s, r, s] -> [r]       + [s, s, r, s]
            [r, r, s, r, s] -> [r]       + [r, s, r, s]
            [s, s, r, s, s] -> [s, s]    + [r, s, s]
            [s, r, r, s, s] -> [s]       + [r, r, s, s]
            [s, s, s]       -> [s, s, s] + []
        """     

    # def generate_grouped_statements(self) -> List[List[Statement]]:
    #     entry_node: Statement = self.statements[0]
    #     assert type(entry_node.block) == nodes.FunctionDef

    #     grouped_statements = []
    #     continuation = self.statements[1:]
    #     while len(continuation) > 0:
    #         first_half, continuation = self.split_statements(continuation)
    #         grouped_statements.append(first_half)

    #     self._grouped_statements = grouped_statements
    #     return grouped_statements
    
    # def split_statements(self, statements: list[Statement]) -> tuple[list[Statement], list[Statement]]:
    #     """ 
    #     Split a list of statements, by grouping together statements that are not remote calls.

    #     As an example, suppose r and s are both statements, where r is a remote call and s is not.

    #     Here is how the list gets split:
    #         [r, s, s, r, s] -> [r]       + [s, s, r, s]
    #         [s, s, r, s, s] -> [s, s]    + [r, s, s]
    #         [s, s, s]       -> [s, s, s] + []
    #     """     
    #     assert len(statements) > 0

    #     if statements[0].is_remote():
    #         return [statements[0]], statements[1:]
        
    #     # find the next remote call
    #     i = 0
    #     first_half = []
    #     while i < len(statements) and not statements[i].is_remote():
    #         first_half.append(statements[i])
    #         i += 1

    #     continuation = statements[i:]
    #     return first_half, continuation
    
    # def build(self, dataflows: dict[DataflowRef, DataFlow], op_name: str) -> tuple[DataFlow, List[LocalBlock]]:
    #     self.build_cfg()

    #     self.generate_grouped_statements()
        
    #     blocks = []
    #     block_num = 0

    #     df_ref = DataflowRef(op_name, self.cfg.name)
    #     df = dataflows[df_ref]

    #     last_node = None
    #     for split in self._grouped_statements:
    #         if len(split) == 1 and split[0].is_remote():
    #             # Entity call
    #             node = to_entity_call(split[0], self.type_map, dataflows)
    #         else:
    #             # Group statements together, into a block
    #             block = LocalBlock(split, self.cfg.name, block_num, op_name)
    #             block_num += 1
    #             node = block.to_node()
    #             blocks.append(block)
          

    #         if last_node == None:
    #             last_node = node
    #             df.add_node(node)
    #             df.entry = [node]
    #         else:
    #             df.add_edge(Edge(last_node, node))
    #             last_node = node

    #     return df, blocks

