from textwrap import dedent

import networkx as nx 

from klara.core.cfg import Cfg
from klara.core import nodes

from cascade.dataflow.dataflow import DataFlow, DataflowRef
from cascade.frontend.ast_visitors.extract_type_visitor import ExtractTypeVisitor
from cascade.frontend.dataflow_analysis.dataflow_graph_builder import DataflowGraphBuilder
from cascade.frontend.generator.generate_split_functions import GenerateSplitFunctions, GroupStatements
from cascade.frontend.generator.split_function import SplitFunction2, to_entity_call
from cascade.frontend.intermediate_representation import Statement, StatementDataflowGraph
from cascade.frontend.util import setup_cfg

def test_call_entity():
    program: str = dedent("""
    class Test:
                          
        def get_total(item1: Stock, item2: Stock):
            a = item1.get_quantity()
            b = item2.get_quantity()           
            return a+b""")
    cfg: Cfg = setup_cfg(program)
    blocks = cfg.block_list
    test_class: nodes.Block = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    sf = GroupStatements(get_total)
    sf.build_cfg()
    
    dataflows = {
        DataflowRef("Test", "get_total"): DataFlow("get_total", "Test", ["item1", "item2"]),
        DataflowRef("Stock", "get_quantity"): DataFlow("get_quantity", "Stock", [])
    }

    df, blocks = sf.build(dataflows, "Test")
    print(df.to_dot())
    print(blocks)
    
def test_simple_block():
    program: str = dedent("""
    class Test:
                          
        def add(x: int, y: int):          
            return x+y""")
    cfg: Cfg = setup_cfg(program)
    blocks = cfg.block_list
    test_class: nodes.Block = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    sf = GroupStatements(get_total)
    
    dataflows = {
        DataflowRef("Test", "add"): DataFlow("get_total", "Test", ["x", "y"]),
    }

    df, blocks = sf.build(dataflows, "Test")

    assert len(blocks) == 1
    assert blocks[0].call({"x_0": 3, "y_0":5 }, None) == 8
