
import os
import sys


# import cascade
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))


from cascade.runtime.python_runtime import PythonClientSync, PythonRuntime
from dataclasses import dataclass
from typing import Any
from cascade.dataflow.dataflow import CallEntity, CallLocal, CollectNode, DataFlow, DataflowRef, Edge, Event, InitClass, InvokeMethod, Node, OpNode, StatelessOpNode
from cascade.dataflow.operator import Block, StatefulOperator, StatelessOperator

class Stock:
    def __init__(self, item: str, quantity: int):
        self.item = item
        self.quantity = quantity

    def get_quantity(self):
        return self.quantity

def get_quantity_compiled_0(variable_map: dict[str, Any], state: Stock) -> Any:
    return state.quantity

stock_op = StatefulOperator(
    Stock,
    {
        "get_quantity_compiled_0": Block(function_call=get_quantity_compiled_0, var_map_writes=[], var_map_reads=[], name="get_quantity_compiled")
    },
    {},
    keyby="item"
)

def stock_op_df():
    df = DataFlow("get_quantity", "Stock")
    n0 = CallLocal(InvokeMethod("get_quantity_compiled_0"))
    df.entry = [n0]
    return df

def stock_op_init_df():
    df = DataFlow("__init__", "Stock")
    n0 = CallLocal(InitClass())
    df.entry = [n0]
    return df

stock_op.dataflows["get_quantity"] = stock_op_df()
stock_op.dataflows["__init__"] = stock_op_init_df()


class Adder:
    @staticmethod
    def add(a, b):
        return a + b

def add_compiled_0(variable_map: dict[str, Any]) -> Any:
    return variable_map["a"] + variable_map["b"]
    
adder_op = StatelessOperator(
    Adder,
    {
        "add_compiled_0": Block(function_call=add_compiled_0, var_map_reads=["a", "b"], var_map_writes=[], name="add_compiled_0")
    },
    {}
)

def adder_df():
    df = DataFlow("add", "Adder")
    n0 = CallLocal(InvokeMethod("add_compiled_0"))
    df.entry = [n0]
    return df

adder_op.dataflows["add"] = adder_df()


class Test:
    @staticmethod
    def get_total(item1: Stock, item2: Stock):
        x = item1.get_quantity()
        y = item2.get_quantity()
        total_adder = Adder.add(x, y)
        total = x + y
        assert total == total_adder
        return total

def get_total_compiled_0(variable_map):
    total = variable_map["x"] + variable_map["y"]
    assert total == variable_map["total_adder"]
    return total

def test_parallelize():
    test_op = StatelessOperator(
        Test,
        {
            "get_total_compiled_0": Block(
                function_call=get_total_compiled_0, 
                var_map_writes=[], 
                var_map_reads=["x", "y", "total_adder"],
                name="get_total_compiled_0")
        },
        {}
    )

    df = DataFlow("get_total", "Test")
    n0 = CallEntity(DataflowRef("get_quantity", "Stock"), {"item": "item1"}, assign_result_to="x")
    n1 = CallEntity(DataflowRef("get_quantity", "Stock"), {"item": "item2"}, assign_result_to="y")
    n2 = CallEntity(DataflowRef("add", "Adder"), {"a": "x", "b": "y"}, assign_result_to="total_adder")
    n3 = CallLocal(InvokeMethod("get_total_compiled_0"))
    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n1, n2))
    df.add_edge(Edge(n2, n3))

    df.entry = [n0]
    test_op.dataflows[df.name] = df
    print(df)
    print(df.nodes)

    df = parallelize(test_op.dataflows[df.name])
    df.name = "get_total_parallel"
    test_op.dataflows[df.name] = df

    runtime = PythonRuntime()
    runtime.add_stateless_operator(test_op)
    runtime.add_stateless_operator(adder_op)
    runtime.add_operator(stock_op)
    runtime.run()

    client = PythonClientSync(runtime)

    event = stock_op.dataflows["__init__"].generate_event({"item": "fork", "quantity": 10})
    result = client.send(event) 
    

    event = stock_op.dataflows["__init__"].generate_event({"item": "spoon", "quantity": 20})
    result = client.send(event) 

    event = test_op.dataflows["get_total"].generate_event({"item1": "fork", "item2": "spoon"})
    result = client.send(event)
    assert result == 30

    event = test_op.dataflows["get_total_parallel"].generate_event({"item1": "fork", "item2": "spoon"})
    result = client.send(event)
    print(result)
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

    updated = DataFlow(df.name)
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
