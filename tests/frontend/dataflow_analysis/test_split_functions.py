from textwrap import dedent

import networkx as nx 

from klara.core.cfg import Cfg
from klara.core import nodes

from cascade.dataflow.dataflow import DataFlow, DataflowRef
from cascade.frontend.ast_visitors.extract_type_visitor import ExtractTypeVisitor
from cascade.frontend.dataflow_analysis.dataflow_graph_builder import ControlFlowGraphBuilder
from cascade.frontend.generator.generate_split_functions import GenerateSplitFunctions, GroupStatements
from cascade.frontend.generator.split_function import SplitFunction2, to_entity_call
from cascade.frontend.intermediate_representation import Statement, ControlFlowGraph
from cascade.frontend.util import setup_cfg

def test_split_functions():
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


    # TODO: Check
    statements = sf.generate_grouped_statements()

    df, blocks = sf.build(dataflows, "Test")
    print(df.to_dot())
    print(blocks)