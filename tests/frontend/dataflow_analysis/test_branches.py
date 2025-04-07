from textwrap import dedent

from cascade.dataflow.dataflow import DataFlow, DataflowRef, IfNode
from cascade.frontend.generator.dataflow_builder import DataflowBuilder
from cascade.frontend.util import setup_cfg
from klara.core import nodes


def test_easy_branching():
    program: str = dedent("""
    class User:      
        def buy_item(self, item: 'Item') -> int:
            item_price = item.get_price()
            cond = self.balance - item_price >= 0
            if cond:
                self.balance = self.balance - item_price
            else:
                x = 10
            return self.balance""")
    cfg = setup_cfg(program)
    blocks = cfg.block_list
    test_class = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    sf = DataflowBuilder(get_total)
    
    dataflows = {
        DataflowRef("User", "buy_item"): DataFlow("buy_item", "User", ["item"]),
        DataflowRef("User", "__init__"): DataFlow("__init__", "User", ["username", "balance"]),
        DataflowRef("Item", "get_price"): DataFlow("get_price", "Item", []),
    }

    df = sf.build(dataflows, "User")
    print(df.to_dot())
    assert len(df.nodes) == 6
    ifnode = None
    for node in df.nodes.values():
        if isinstance(node, IfNode):
            assert ifnode is None
            ifnode = node
    
    assert ifnode is not None
    assert len(ifnode.outgoing_edges) == 2


def test_complex_predicate():
    program: str = dedent("""
    class User:
       def buy_item(self, item: 'Item') -> int:
            item_price = item.get_price()
            if self.balance >= item_price:
                self.balance = self.balance - item_price
            else:
                x = 10
            return self.balance""")
    cfg = setup_cfg(program)
    blocks = cfg.block_list
    test_class = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    sf = DataflowBuilder(get_total)
    
    dataflows = {
        DataflowRef("User", "buy_item"): DataFlow("buy_item", "User", ["item"]),
        DataflowRef("User", "__init__"): DataFlow("__init__", "User", ["username", "balance"]),
        DataflowRef("Item", "get_price"): DataFlow("get_price", "Item", []),
    }

    df = sf.build(dataflows, "User")
    print(df.to_dot())
    assert len(df.nodes) == 6, "complex predicate should create a temp variable assignment"


def test_multiple_return():
    program: str = dedent("""
    class User:
        def buy_item(self, item: 'Item') -> int:
            item_price = item.get_price()
            cond = self.balance - item_price >= 0
            if cond:
                item_price = item.get_price()
                self.balance = self.balance - item_price
                return "item bought"
            else:
                item_price = item.get_price()
                msg = str(item_price) + " is too expensive!"
                return msg""")
    cfg = setup_cfg(program)
    blocks = cfg.block_list
    test_class = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    sf = DataflowBuilder(get_total)
    
    dataflows = {
        DataflowRef("User", "buy_item"): DataFlow("buy_item", "User", ["item"]),
        DataflowRef("User", "__init__"): DataFlow("__init__", "User", ["username", "balance"]),
        DataflowRef("Item", "get_price"): DataFlow("get_price", "Item", []),
    }

    df = sf.build(dataflows, "User")
    print(df.to_dot())

def test_no_else():
    program: str = dedent("""
    class User:     
        def buy_item(self, item: 'Item') -> int:
            item_price = item.get_price()
            cond1 = self.balance - item_price >= 0
            if cond1:
                item_price = item.get_price()
                self.balance = self.balance - item_price
            x = 0
            return item_price""")
    cfg = setup_cfg(program)
    blocks = cfg.block_list
    test_class = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    sf = DataflowBuilder(get_total)
    
    dataflows = {
        DataflowRef("User", "buy_item"): DataFlow("buy_item", "User", ["item"]),
        DataflowRef("User", "__init__"): DataFlow("__init__", "User", ["username", "balance"]),
        DataflowRef("Item", "get_price"): DataFlow("get_price", "Item", []),
    }

    df = sf.build(dataflows, "User")
    print(df.to_dot())
    assert len(df.nodes) == 6

def test_nested():
    program: str = dedent("""
    class User:     
        def buy_item(self, item: 'Item') -> int:
            item_price = item.get_price()
            cond1 = self.balance - item_price >= 0
            if cond1:
                item_price = item.get_price()
                if True:
                    x = 20
                self.balance = self.balance - item_price
                return "item bought"
            else:
                if True:
                    x = 20
                else:
                    x = 30
                item_price = item.get_price()
                msg = "item is too expensive!"
                return msg""")
    cfg = setup_cfg(program)
    blocks = cfg.block_list
    test_class = blocks[2] 
    get_total: nodes.FunctionDef = test_class.blocks[1].ssa_code.code_list[0]

    sf = DataflowBuilder(get_total)
    
    dataflows = {
        DataflowRef("User", "buy_item"): DataFlow("buy_item", "User", ["item"]),
        DataflowRef("User", "__init__"): DataFlow("__init__", "User", ["username", "balance"]),
        DataflowRef("Item", "get_price"): DataFlow("get_price", "Item", []),
    }

    df = sf.build(dataflows, "User")
    print(df.to_dot())
    assert len(df.nodes) == 12