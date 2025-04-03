from dataclasses import dataclass
from textwrap import dedent

import networkx as nx 

from klara.core.cfg import Cfg
from klara.core import nodes

from cascade.dataflow.dataflow import CallEntity, CallLocal, DataFlow, DataflowRef
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

    ## TODO: check blocks/df
    assert len(df.nodes) == 3
    assert len(df.entry) == 1
    entry = df.entry[0]
    assert isinstance(entry, CallEntity)
    next = df.get_neighbors(entry)
    assert len(next) == 1
    next = next[0]
    assert isinstance(next, CallEntity)
    next = df.get_neighbors(next)
    assert len(next) == 1
    next = next[0]
    assert isinstance(next, CallLocal)

    
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


def test_state():
    program = dedent("""
class User: 
    def buy_item(self, item: 'Item') -> bool:
        item_price = item.get_price() # SSA
        self.balance = self.balance - item_price
        return self.balance >= 0
""")

    cfg: Cfg = setup_cfg(program)
    blocks = cfg.block_list
    user_class: nodes.Block = blocks[2] 
    buy_item: nodes.FunctionDef = user_class.blocks[1].ssa_code.code_list[0]

    sf = GroupStatements(buy_item)
    
    dataflows = {
        DataflowRef("User", "buy_item"): DataFlow("buy_item", "User", ["item"]),
        DataflowRef("Item", "get_price"): DataFlow("get_price", "Item", []),
    }

    df, blocks = sf.build(dataflows, "User")

    assert len(blocks) == 1
    func = blocks[0].call
    print(blocks[0].raw_method_string)
    
    @dataclass
    class User:
        username: str
        balance: int

    func = blocks[0].call

    user = User("a", 20)
    func({"item_price_0": 10}, user.__dict__)
    assert user.balance == 10

    func({"item_price_0": 13}, user.__dict__)
    assert user.balance == -3

def test_dict_state():
    program = dedent("""
class ComposeReview:
    def upload_unique_id(self, review_id: int):
        self.review_data["review_id"] = review_id
""")

    cfg: Cfg = setup_cfg(program)
    blocks = cfg.block_list
    user_class: nodes.Block = blocks[2] 
    upload_unique: nodes.FunctionDef = user_class.blocks[1].ssa_code.code_list[0]
    
    sf = GroupStatements(upload_unique)
    
    dataflows = {
        DataflowRef("ComposeReview", "upload_unique_id"): DataFlow("upload_unique_id", "ComposeReview", ["review_id"]),
        DataflowRef("ComposeReview", "__init__"): DataFlow("__init__", "ComposeReview", ["req_id"]),
    }

    df, blocks = sf.build(dataflows, "ComposeReview")

    assert len(blocks) == 1
        
    
    @dataclass
    class User:
        req_id: str
        review_data: dict

    func = blocks[0].call

    print(blocks[0].raw_method_string)

    user = User("req", {})
    func({"review_id_0": 123}, user.__dict__)
    assert user.review_data["review_id"] == 123
