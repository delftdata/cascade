from textwrap import dedent

from klara.core.cfg import Cfg
from klara.core import nodes

from cascade.frontend.cfg.cfg_builder import ControlFlowGraphBuilder
from cascade.frontend.cfg import Statement, ControlFlowGraph
from cascade.preprocessing import setup_cfg


def test_linear_program():
    program: str = dedent("""
    class Test:
                          
        def get_total(item1: Stock, item2: Stock):
            q1 = item1.get_quantity()
            q2 = item2.get_quantity()
            total = Adder.add(q1, q2)
            return total""")
    
    cfg, _ = setup_cfg(program)
    blocks = cfg.block_list
    test_class: nodes.Block = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    # TODO: check that the produced ssa code made variables for 
    #  - item1.get_quantity()
    #  - item2.get_quantity()
    df: ControlFlowGraph = ControlFlowGraphBuilder.build([get_total] + get_total.body, globals=[])
    for n in df.graph.nodes:
        print(n)
    for u, v in df.graph.edges:
        print(u.block_num, v.block_num)
    # print(df.graph.edges)

def test_ssa():
    program: str = dedent("""
    class Test:
                          
        def get_total(item1: Stock, item2: Stock):
            total = Adder.add(item1.get_quantity(), item2.get_quantity())
            return total""")
    
    cfg, _ = setup_cfg(program)
    blocks = cfg.block_list
    test_class: nodes.Block = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    # TODO: check that the produced ssa code made variables for 
    #  - item1.get_quantity()
    #  - item2.get_quantity()
    df: ControlFlowGraph = ControlFlowGraphBuilder.build([get_total] + get_total.body, globals=[])
    print(df.graph.nodes)
    print(df.graph.edges)


def test_if_else_branches():
    program: str = dedent("""
    class Test:
                          
        def test_branches(item1: Stock, item2: Stock):
            q = item1.get_quantity()
            cond = q < 10
            if cond:
                a = item2.get_quantity()
            else:
                a = 0
            return a""")
    
    cfg, _ = setup_cfg(program)
    blocks = cfg.block_list
    print(blocks)
    test_class: nodes.Block = blocks[2] 
    test: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    # TODO: check that the produced ssa code made variables for 
    #  - item1.get_quantity()
    #  - item2.get_quantity()
    df: ControlFlowGraph = ControlFlowGraphBuilder.build([test] + test.body, globals=[])
    # print(df.graph.nodes)
    # print(df.graph.edges)