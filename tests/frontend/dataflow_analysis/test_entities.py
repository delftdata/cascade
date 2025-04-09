from dataclasses import dataclass
from textwrap import dedent


from klara.core.cfg import Cfg
from klara.core import nodes

from cascade.dataflow.dataflow import CallEntity, CallLocal, DataFlow, DataflowRef

from cascade.frontend.generator.dataflow_builder import DataflowBuilder
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
    test_class = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    sf = DataflowBuilder(get_total)
    sf.build_cfg()
    
    dataflows = {
        DataflowRef("Test", "get_total"): DataFlow("get_total", "Test", ["item1", "item2"]),
        DataflowRef("Stock", "get_quantity"): DataFlow("get_quantity", "Stock", [])
    }

    df = sf.build(dataflows, "Test")

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
    test_class = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    sf = DataflowBuilder(get_total)
    
    dataflows = {
        DataflowRef("Test", "add"): DataFlow("get_total", "Test", ["x", "y"]),
    }

    df = sf.build(dataflows, "Test")

    assert len(df.blocks) == 1
    assert list(df.blocks.values())[0].call_block({"x_0": 3, "y_0":5 }, None) == 8


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
    user_class = blocks[2] 
    buy_item: nodes.FunctionDef = user_class.blocks[1].ssa_code.code_list[0]

    sf = DataflowBuilder(buy_item)
    
    dataflows = {
        DataflowRef("User", "buy_item"): DataFlow("buy_item", "User", ["item"]),
        DataflowRef("Item", "get_price"): DataFlow("get_price", "Item", []),
    }

    df = sf.build(dataflows, "User")

    blocks = list(df.blocks.values())

    assert len(blocks) == 1
    func = blocks[0].call_block
    print(blocks[0].function_string)
    
    @dataclass
    class User:
        username: str
        balance: int

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
    user_class = blocks[2] 
    upload_unique: nodes.FunctionDef = user_class.blocks[1].ssa_code.code_list[0]
    
    sf = DataflowBuilder(upload_unique)
    
    dataflows = {
        DataflowRef("ComposeReview", "upload_unique_id"): DataFlow("upload_unique_id", "ComposeReview", ["review_id"]),
        DataflowRef("ComposeReview", "__init__"): DataFlow("__init__", "ComposeReview", ["req_id"]),
    }

    df = sf.build(dataflows, "ComposeReview")


    blocks = list(df.blocks.values())
    assert len(blocks) == 1
        
    
    @dataclass
    class ComposeReview:
        req_id: str
        review_data: dict

    func = blocks[0].call_block

    print(blocks[0].function_string)

    compose_review = ComposeReview("req", {})
    func({"review_id_0": 123}, compose_review.__dict__)
    assert compose_review.review_data["review_id"] == 123


def test_import():
    program = dedent("""
class Randomer:
    @staticmethod
    def rand():
        r = random.random()
        return r
""")

    cfg: Cfg = setup_cfg(program)
    blocks = cfg.block_list
    user_class = blocks[2] 
    upload_unique: nodes.FunctionDef = user_class.blocks[1].ssa_code.code_list[0]
    
    import random
    sf = DataflowBuilder(upload_unique, {'random': random})
    sf.build_cfg()
    for node in sf.cfg.get_nodes():
        print(node)
    
    dataflows = {
        DataflowRef("Randomer", "rand"): DataFlow("rand", "Randomer", []),
    }

    df = sf.build(dataflows, "Randomer")

    for block in df.blocks.values():
        print(block.function_string)

    rands = {df.blocks['rand_0'].call_block(variable_map={}, state=None) for x in range(10)}
    assert len(rands) == 10