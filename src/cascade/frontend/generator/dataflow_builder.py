import networkx as nx

from cascade.dataflow.dataflow import DataFlow, DataflowRef, IfNode
from cascade.frontend.ast_visitors.extract_type_visitor import ExtractTypeVisitor
from cascade.frontend.cfg.cfg_builder import ControlFlowGraphBuilder
from cascade.frontend.cfg import Statement, ControlFlowGraph
from cascade.frontend.generator.local_block import LocalBlock, to_entity_call


from klara.core import nodes

def split_statements_once(statements: list[Statement]) -> tuple[list[Statement], list[Statement]]:
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

def split_statements(statements: list[Statement]) -> list[tuple[Statement,...]]:
    grouped_statements = []
    continuation = statements
    while len(continuation) > 0:
        first_half, continuation = split_statements_once(continuation)
        grouped_statements.append(tuple(first_half))

    return grouped_statements    

def split_cfg(blocked_statement_graph: nx.DiGraph) -> nx.DiGraph:
    split_graph: nx.DiGraph = blocked_statement_graph.copy()
    for node in list(split_graph.nodes):
        in_nodes = split_graph.predecessors(node)
        out_nodes = split_graph.successors(node)

        # create the new nodes
        new_nodes = split_statements(list(node))
        split_graph.remove_node(node)
        split_graph.add_nodes_from(new_nodes)

        # connect the inner edges
        u = new_nodes[0]
        for v in new_nodes[1:]:
            split_graph.add_edge(u, v)
            u = v

        # connect the outer edges
        for u in in_nodes:
            split_graph.add_edge(u, new_nodes[0])
        for v in out_nodes:
            split_graph.add_edge(new_nodes[-1], v)

    return split_graph


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
    elif len(succ) == 2:
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
        
        if len(succ_then) == 1 and len(succ_orelse) == 1:
            assert succ_orelse[0] == succ_then[0]

        assert len(succ_then) <= 1
        assert len(succ_orelse) <= 1

        

        # add then and orelse blocks
        graph.add_edges_from(then_blocked_graph.edges())
        graph.add_edges_from(orelse_blocked_graph.edges())
        
        # connect them to this node
        first_then = list(then_blocked_graph.nodes)[0]
        first_orelse = list(orelse_blocked_graph.nodes)[0]
        graph.add_edge(last_node, first_then)
        graph.add_edge(last_node, first_orelse)

        # connect the rest of the graph at the end (recursively)
        if len(succ_then) == 1 or len(succ_orelse) == 1:
            try:
                first_finally = succ_orelse[0]
            except IndexError:
                first_finally = succ_then[0]
            finally_graph = blocked_cfg(statement_graph, first_finally)
            graph.add_edges_from(finally_graph.edges())
            first_finally = list(finally_graph.nodes)[0]
        
            graph.add_edge(last_then, first_finally)
            graph.add_edge(last_orelse, first_finally)
        
        return graph
    else:
        raise ValueError(f"We expect a CFG node to have max 2 successors, got {succ}")



class DataflowBuilder:
    def __init__(self, function_def: nodes.FunctionDef):
        self.function_def = function_def
        self.name = self.function_def.name


    def build_cfg(self):
        cfg: ControlFlowGraph = ControlFlowGraphBuilder.build([self.function_def] + self.function_def.body)
        self.type_map = ExtractTypeVisitor.extract(self.function_def)
        cfg.name = self.function_def.name

        entry_node: Statement = cfg.get_source_nodes()[0]
        assert type(entry_node.block) == nodes.FunctionDef
        cfg.remove_node(entry_node)
        self.cfg = cfg

        self.blocked_cfg = split_cfg(blocked_cfg(cfg.graph, cfg.get_single_source()))
    
    def build_df(self, dataflows: dict[DataflowRef, DataFlow], op_name: str) -> DataFlow:
        df_ref = DataflowRef(op_name, self.name)
        df = dataflows[df_ref]

        node_id_map = {}

        block_num = 0
        is_entry = True
        for statement_block in self.blocked_cfg.nodes:
            if len(statement_block) == 1 and statement_block[0].is_remote():
                node = to_entity_call(statement_block[0], self.type_map, dataflows)
            elif len(statement_block) == 1 and statement_block[0].is_predicate:
                node = IfNode()
            else:
                block = LocalBlock(list(statement_block), self.name, block_num, op_name)
                block_num += 1
                node = block.to_node()
                df.add_block(block)
            node_id_map[statement_block] = node.id
            df.add_node(node)

            if is_entry:
                df.entry = [node]
                is_entry = False

        for source, target, if_result in self.blocked_cfg.edges.data('type', default=None):
            source_id = node_id_map[source]
            target_id = node_id_map[target]
            df.add_edge_refs(source_id, target_id, if_result)

        return df
    
    
    def build(self, dataflows: dict[DataflowRef, DataFlow], op_name: str) -> DataFlow:
        self.build_cfg()

        return self.build_df(dataflows, op_name)

