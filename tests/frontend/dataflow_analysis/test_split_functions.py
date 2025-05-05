from textwrap import dedent

import networkx as nx 

from klara.core.cfg import Cfg
from klara.core import nodes

from cascade.dataflow.dataflow import DataFlow, DataflowRef
from cascade.frontend.generator.dataflow_builder import DataflowBuilder, blocked_cfg, split_cfg
from cascade.frontend.cfg.control_flow_graph import ControlFlowGraph
from cascade.preprocessing import setup_cfg

def test_entity_calls():
    program: str = dedent("""
    class Test:
                          
        def get_total(item1: Stock, item2: Stock, y: int):
            a = 10
            b = a + 3
            x = item1.get_quantity()
            y = item2.get_quantity()
            total = Adder.add(x, y)
            total = total + a + b
            total = total - 23            
            return total""")
    cfg, _ = setup_cfg(program)
    blocks = cfg.block_list
    test_class: nodes.Block = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    
    dataflows = {
        DataflowRef("Adder", "add"): DataFlow("add", "Adder", ["a", "b"]),
        DataflowRef("Stock", "get_quantity"): DataFlow("get_quantity", "Item", []),
        DataflowRef("Test", "get_total"): DataFlow("get_total", "Test", [])
    }
    sf = DataflowBuilder(get_total, dataflows)
    sf.build_cfg()
    df = sf.build_df("Test")

    print(df.to_dot())
    for block in df.blocks.values():
        print(block.function_string)

    # TODO: Check # entity calls, # of local calls
    assert len(df.nodes) == 6
    assert len(df.blocks) == 2

def test_branching():
    program: str = dedent("""
    class Test:       
        def test_branching(self) -> int:
            pre = 10
            if True:
                then = 20
                and_then = 10
            else:
                orelse = 30
                orelser = 30
            post = 40
            return 50""")
    cfg, _ = setup_cfg(program)
    blocks = cfg.block_list
    test_class: nodes.Block = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    dataflows = {
        DataflowRef("Test", "test_branching"): DataFlow("test_branching", "Test", [])
    }

    sf = DataflowBuilder(get_total, dataflows)
    sf.build_cfg()
    print(sf.cfg.to_dot())
    new = blocked_cfg(sf.cfg.graph, sf.cfg.get_single_source())

    print_digraph(new)

    print_digraph(split_cfg(new))

    assert len(new.nodes) == 5

    df = sf.build_df("Test")

    print(df.to_dot())
    for block in df.blocks.values():
        print(block.function_string)
    assert len(df.nodes) == 6
    assert len(df.blocks) == 4

def print_digraph(graph: nx.DiGraph):
    for node in graph.nodes:
        for s in node:
            print(s.block_num, end=" ")
        print()
    for u, v, c in graph.edges.data('type', default=None):
        print(u[0].block_num, end=" ")
        print("->", end= " ")
        print(v[0].block_num, end=" ")
        if c is not None:
            print(f' [label="{c}"]', end=" ")
        print()

def test_branching_with_entity_calls():
    program: str = dedent("""
    class Test:       
        def test_branching(self) -> int:
            pre = 10
            if True:
                then = 10
                and_then = 10
            else:
                orelse = 30
                y = 10
                orelser = Entity.call()
                orelserer = 40
                x = 10
            post = 40
            return 50""")
    cfg, _ = setup_cfg(program)
    blocks = cfg.block_list
    test_class: nodes.Block = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    dataflows = {
        DataflowRef("Test", "test_branching"): DataFlow("test_branching", "Test", []),
        DataflowRef("Entity", "call"): DataFlow("call", "Entity", [])
    }
    sf = DataflowBuilder(get_total, dataflows)
    sf.build_cfg()
    print(sf.cfg.to_dot())
    new = blocked_cfg(sf.cfg.graph, sf.cfg.get_single_source())

    assert len(list(new.nodes)) == 5
    print(new.nodes)
    new_split = split_cfg(new)
    print(new_split.nodes)
    assert len(list(new_split.nodes)) == 8
    



    # TODO: Check # entity calls, # of blocks, # of local calls

    df = sf.build_df("Test")
    print(df.to_dot())
    for block in df.blocks.values():
        print(block.function_string)

    assert len(df.nodes) == 8
    assert len(df.blocks) == 5

def test_block_merging():
    raise NotImplementedError()