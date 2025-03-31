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
        DataflowRef("Stock", "get_quantity"): DataFlow("get_quantity", "Item", [])
    }


    # TODO: Check
    statements = sf.generate_grouped_statements()

    df, blocks = sf.build(dataflows, "Test")
    print(df.to_dot())
    print(blocks)
    


# [
#     Statement(block_num=0, block=Function get_total in scope Class "Test" in scope Module, targets=[item1_0, item2_0], values=[item1_0, item2_0], remote_call=False, attribute=None), 
#     Statement(block_num=1, block=Assign: (a_0,) = 10, targets=[a_0], values=[], remote_call=False, attribute=None), 
#     Statement(block_num=2, block=Assign: (b_0,) = BinOp: a_0 + 3, targets=[b_0], values=[a_0], remote_call=False, attribute=None), 
#     Statement(block_num=3, block=Assign: (x_0,) = Call: item1_0.get_quantity(()), targets=[x_0], values=[item1_0], remote_call=True, attribute=item1_0.get_quantity), 
#     Statement(block_num=4, block=Assign: (y_0,) = Call: item2_0.get_quantity(()), targets=[y_0], values=[item2_0], remote_call=True, attribute=item2_0.get_quantity), 
#     Statement(block_num=5, block=Assign: (total_0,) = Call: Adder.add((x_0, y_0)), targets=[total_0], values=[Adder, x_0, y_0], remote_call=True, attribute=Adder.add), 
#     Statement(block_num=6, block=<klara.core.node_classes.AugAssign object at 0x7f48dda7a990>, targets=[total_1], values=[a_0, b_0], remote_call=False, attribute=None),
#     Statement(block_num=7, block=<klara.core.node_classes.AugAssign object at 0x7f48dda7a950>, targets=[total_2], values=[], remote_call=False, attribute=None), 
#     Statement(block_num=8, block=<klara.core.node_classes.Return object at 0x7f4982f81a90>, targets=[], values=[total_2], remote_call=False, attribute=None)]

# [
#     (0, 3), 
#     (0, 4), 
#     (3, 5), 
#     (4, 5), 
#     (1, 2), 
#     (1, 6), 
#     (2, 6), 
#     (7, 8)]