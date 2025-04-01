
import os
import sys


# import cascade
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))


from cascade.runtime.python_runtime import PythonClientSync, PythonRuntime
from dataclasses import dataclass
from typing import Any
from cascade.dataflow.dataflow import CallEntity, CallLocal, CollectNode, DataFlow, DataflowRef, Edge, Event, InitClass, InvokeMethod, Node, OpNode, StatelessOpNode
from cascade.dataflow.operator import Block, StatefulOperator, StatelessOperator

import cascade

def test_parallelize():
    cascade.core.clear() # clear cascadeds registerd classes.
    assert not cascade.core.registered_classes, "Registered classes should be empty before importing a Cascade \
                                                    Module"
    # import the module
    import_module_name: str = 'test_ops'
    exec(f'import tests.optimizations.{import_module_name}')
    
    cascade.core.init()

    print(cascade.core.operators)
    test_op = cascade.core.operators["Test"]
    adder_op = cascade.core.operators["Adder"]
    stock_op = cascade.core.operators["Stock"]
    df = test_op.dataflows["get_total"]
    print(df)
    print(df.nodes)

    df = parallelize(test_op.dataflows[df.name])
    df.name = "get_total_parallel"
    test_op.dataflows[df.name] = df

    assert len(test_op.dataflows["get_total_parallel"].entry) == 2
    assert len(test_op.dataflows["get_total"].entry) == 1

    runtime = PythonRuntime()
    runtime.add_stateless_operator(test_op)
    runtime.add_stateless_operator(adder_op)
    runtime.add_operator(stock_op)
    runtime.run()

    client = PythonClientSync(runtime)

    event = stock_op.dataflows["__init__"].generate_event({"item": "fork", "quantity": 10}, key="fork")
    result = client.send(event) 
    

    event = stock_op.dataflows["__init__"].generate_event({"item": "spoon", "quantity": 20}, key="spoon")
    result = client.send(event) 

    event = test_op.dataflows["get_total"].generate_event({"item1_0": "fork", "item2_0": "spoon"})
    result = client.send(event)
    assert result == 30

    event = test_op.dataflows["get_total_parallel"].generate_event({"item1_0": "fork", "item2_0": "spoon"})
    result = client.send(event)
    assert result == 30

@dataclass
class AnnotatedNode:
    node: Node
    reads: list[str]
    writes: list[str]
    
import networkx as nx
def parallelize(df: DataFlow):    
    # create the dependency graph
    ans = []
    # since we use SSA, every variable has exactly one node that writes it
    write_nodes = {} 
    graph = nx.DiGraph()
    for node in df.nodes.values():
        if isinstance(node, CallEntity):
            reads = list(node.variable_rename.values())
            writes = [result] if (result := node.assign_result_to) else []
        elif isinstance(node, CallLocal):
            method = df.get_operator().methods[node.method.method_name]
            reads = method.var_map_reads
            writes = method.var_map_writes
        else:
            raise ValueError(f"unsupported node type: {type(node)}")
        
        write_nodes.update({var: node.id for var in writes})

        ans.append(AnnotatedNode(node, reads, writes))
        graph.add_node(node.id)

    nodes_with_indegree_0 = set(graph.nodes)
    n_map = df.nodes
    for node in ans:
        for read in node.reads:
            print(read)
            if read in write_nodes:
                # "read" will not be in write nodes if it is part of the arguments
                # a more thorough implementation would not need the if check,
                # and add the arguments as writes to some function entry node
                graph.add_edge(write_nodes[read], node.node.id)
                try:
                    nodes_with_indegree_0.remove(node.node.id)
                except KeyError:
                    pass

    updated = DataFlow(df.name, df.op_name)
    updated.entry = [n_map[node_id] for node_id in nodes_with_indegree_0]
    prev_node = None
    print(nodes_with_indegree_0)

    while len(nodes_with_indegree_0) > 0:
        # remove nodes from graph
        children = []
        for node_id in nodes_with_indegree_0:
            children.extend(graph.successors(node_id))
            graph.remove_node(node_id)
            updated.add_node(n_map[node_id])
            

        # check for new indegree 0 nodes
        next_nodes = set()
        for child in children:
            if graph.in_degree(child) == 0:
                next_nodes.add(child)
        
        if len(nodes_with_indegree_0) > 1:
            # TODO: maybe collect node should just infer from it's predecessors?
            # like it can only have DataFlowNode predecessors
            # TODO: rename DataflowNode to EntityCall
            collect_node = CollectNode()
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

    print(df.to_dot())
    print(updated.to_dot())
    return updated
