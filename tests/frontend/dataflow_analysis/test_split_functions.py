from textwrap import dedent

import networkx as nx 

from klara.core.cfg import Cfg
from klara.core import nodes

from cascade.dataflow.dataflow import DataFlow, DataflowRef
from cascade.frontend.generator.generate_split_functions import GroupStatements, blocked_cfg
from cascade.frontend.intermediate_representation.control_flow_graph import ControlFlowGraph
from cascade.frontend.util import setup_cfg

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
    cfg: Cfg = setup_cfg(program)
    blocks = cfg.block_list
    test_class: nodes.Block = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    
    sf = GroupStatements(get_total)
    sf.build_cfg()
    
    dataflows = {
        DataflowRef("Adder", "add"): DataFlow("add", "Adder", ["a", "b"]),
        DataflowRef("Stock", "get_quantity"): DataFlow("get_quantity", "Item", []),
        DataflowRef("Test", "get_total"): DataFlow("get_total", "Test", [])
    }


    # TODO: Check # entity calls, # of blocks, # of local calls

    df = sf.build_df(dataflows, "Test")
    print(df.to_dot())
    for block in df.blocks.values():
        print(block.to_string())

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
    cfg: Cfg = setup_cfg(program)
    blocks = cfg.block_list
    test_class: nodes.Block = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    sf = GroupStatements(get_total)
    sf.build_cfg()
    print(sf.cfg.to_dot())
    new = blocked_cfg(sf.cfg.graph, sf.cfg.get_single_source())
    for node in new.nodes:
        for s in node:
            print(s.block_num, end=" ")
        print()
    for edge in new.edges:
        for s in edge[0]:
            print(s.block_num, end=" ")
        print("->", end= " ")
        for s in edge[1]:
            print(s.block_num, end=" ")
        print()
    
    dataflows = {
        DataflowRef("Test", "test_branching"): DataFlow("test_branching", "Test", [])
    }


    df = sf.build_df(dataflows, "Test")
    print(df.to_dot())
    for block in df.blocks.values():
        print(block.to_string())
    assert len(df.blocks) == 4
    assert len(df.nodes) == 5

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
    cfg: Cfg = setup_cfg(program)
    blocks = cfg.block_list
    test_class: nodes.Block = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    sf = GroupStatements(get_total)
    sf.build_cfg()
    print(sf.cfg.to_dot())
    new = blocked_cfg(sf.cfg.graph, sf.cfg.get_single_source())
    for node in new.nodes:
        for s in node:
            print(s.block_num, end=" ")
        print()
    for edge in new.edges:
        for s in edge[0]:
            print(s.block_num, end=" ")
        print("->", end= " ")
        for s in edge[1]:
            print(s.block_num, end=" ")
        print()
    
    dataflows = {
        DataflowRef("Test", "test_branching"): DataFlow("test_branching", "Test", []),
        DataflowRef("Entity", "call"): DataFlow("call", "Entity", [])
    }


    # TODO: Check # entity calls, # of blocks, # of local calls

    df = sf.build_df(dataflows, "Test")
    print(df.to_dot())
    for block in df.blocks.values():
        print(block.to_string())

def test_block_merging():
    raise NotImplementedError()