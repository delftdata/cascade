from textwrap import dedent

from klara.core.cfg import Cfg
from klara.core import nodes

from cascade.frontend.dataflow_analysis.dataflow_graph_builder import DataflowGraphBuilder
from cascade.frontend.intermediate_representation import Statement, StatementDataflowGraph
from cascade.frontend.util import setup_cfg


def test_ssa():
    program: str = dedent("""
    class Test:
                          
        def get_total(item1: Stock, item2: Stock):
            total = Adder.add(item1.get_quantity(), item2.get_quantity())
            return total""")
    
    cfg: Cfg = setup_cfg(program)
    blocks = cfg.block_list
    test_class: nodes.Block = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    # TODO: check that the produced ssa code made variables for 
    #  - item1.get_quantity()
    #  - item2.get_quantity()
    df: StatementDataflowGraph = DataflowGraphBuilder.build([get_total] + get_total.body)
    print(df.graph.nodes)
    print(df.graph.edges)
