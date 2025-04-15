import copy
from dataclasses import dataclass
from typing import Any, Tuple
from cascade.dataflow.dataflow import CallRemote, CallLocal, CollectNode, DataFlow, Edge, IfNode, Node
import cascade

@dataclass
class AnnotatedNode:
    node: Node
    reads: set[str]
    writes: set[str]
    

import networkx as nx
def parallelize_until_if(df: DataFlow) -> Tuple[DataFlow, DataFlow]:  
    """Parallelize df, stopping at the first if node.
    The first dataflow returned is the parallelized dataflow up until the first if node. The second dataflow is the rest of the dataflow"""  
    # create the dependency graph
    ans = []
    # since we use SSA, every variable has exactly one node that writes it
    write_nodes = {} 
    graph = nx.DiGraph()
    for node in df.nodes.values():
        if isinstance(node, CallRemote):
            reads = set(node.variable_rename.values())
            writes = {result} if (result := node.assign_result_to) else set()
        elif isinstance(node, CallLocal):
            operator = cascade.core.operators[df.operator_name]
            method = df.blocks[node.method.method_name]
            reads = method.reads
            writes = method.writes
        elif isinstance(node, IfNode):
            break
        else:
            raise ValueError(f"unsupported node type: {type(node)}")
        
        write_nodes.update({var: node.id for var in writes})

        ans.append(AnnotatedNode(node, reads, writes))
        graph.add_node(node.id)

    nodes_with_indegree_0 = set(graph.nodes)
    n_map = copy.deepcopy(df.nodes)
    for node in ans:
        for read in node.reads:
            if read in write_nodes:
                # "read" will not be in write nodes if it is part of the arguments
                # a more thorough implementation would not need the if check,
                # and add the arguments as writes to some function entry node
                graph.add_edge(write_nodes[read], node.node.id)
                try:
                    nodes_with_indegree_0.remove(node.node.id)
                except KeyError:
                    pass

    updated = DataFlow(df.name, df.operator_name)
    updated.entry = [n_map[node_id] for node_id in nodes_with_indegree_0]
    prev_node = None

    rest = copy.deepcopy(df)

    while len(nodes_with_indegree_0) > 0:
        # remove nodes from graph
        children = set()
        for node_id in nodes_with_indegree_0:
            children.update(graph.successors(node_id))
            graph.remove_node(node_id)
            rest.remove_node(n_map[node_id])
            updated.add_node(n_map[node_id])
            

        # check for new indegree 0 nodes
        next_nodes = set()
        for child in children:
            if graph.in_degree(child) == 0:
                next_nodes.add(child)
        
        if len(nodes_with_indegree_0) > 1:
            collect_node = CollectNode(len(nodes_with_indegree_0))
            for node_id in nodes_with_indegree_0:
                if prev_node:
                    updated.add_edge(Edge(prev_node, n_map[node_id]))
                updated.add_edge(Edge(n_map[node_id], collect_node))
            prev_node = collect_node
        else:
            node_id = nodes_with_indegree_0.pop()
            if prev_node:
                updated.add_edge(Edge(prev_node, n_map[node_id]))

            prev_node = n_map[node_id]

        nodes_with_indegree_0 = next_nodes

    return updated, rest
