from typing import Any
from cascade.dataflow.dataflow import DataFlow, Edge, Event, InvokeMethod, MergeNode, OpNode

class DummyUser:
    def __init__(self, key: str, balance: int):
        self.key: str = key
        self.balance: int = balance
    
    def buy_item(self, item: 'DummyItem') -> bool:
        item_price = item.get_price() # SSA
        self.balance -= item_price
        return self.balance >= 0

def buy_item_0_compiled(item_key, *, state: DummyUser, key_stack: list[str]) -> Any:
    key_stack.append(item_key)
    return item_key

def buy_item_1_compiled(item_price: int, *, state: DummyUser, key_stack: list[str]) -> Any:
    key_stack.pop()
    state.balance -= item_price
    return state.balance >= 0

class DummyItem:
    def __init__(self, key: str, price: int):
        self.key: str = key
        self.price: int = price

    def get_price(self) -> int:
        return self.price
    
def get_price_compiled(*args, state: DummyItem, key_stack: list[str]) -> Any:
    key_stack.pop() # final function
    return state.price

################## TESTS #######################

user = DummyUser("user", 100)
item = DummyItem("fork", 5)

def test_simple_df_propogation():
    df = DataFlow("user.buy_item")
    n1 = OpNode(DummyUser, InvokeMethod("buy_item_0"))
    n2 = OpNode(DummyItem, InvokeMethod("get_price"))
    n3 = OpNode(DummyUser, InvokeMethod("buy_item_1"))
    df.add_edge(Edge(n1, n2))
    df.add_edge(Edge(n2, n3))

    event = Event(n1, ["user"], ["fork"], {}, df)

    # Manually propogate
    item_key = buy_item_0_compiled(event.args, state=user, key_stack=event.key_stack)
    next_event = event.propogate(event.key_stack, item_key, None)

    assert len(next_event) == 1
    assert isinstance(next_event[0].target, OpNode)
    assert next_event[0].target.cls == DummyItem
    assert next_event[0].key_stack == ["user", "fork"]
    event = next_event[0]

    item_price = get_price_compiled(event.args, state=item, key_stack=event.key_stack)
    next_event = event.propogate(event.key_stack, item_price, None)

    assert len(next_event) == 1
    assert isinstance(next_event[0].target, OpNode)
    assert next_event[0].target.cls == DummyUser
    assert next_event[0].key_stack == ["user"]
    event = next_event[0]

    positive_balance = buy_item_1_compiled(event.args, state=user, key_stack=event.key_stack)
    next_event = event.propogate(event.key_stack, None, None)
    assert next_event[0].key_stack == []


def test_merge_df_propogation():
    df = DataFlow("user.buy_2_items")
    n0 = OpNode(DummyUser, InvokeMethod("buy_2_items_0"))
    n1 = OpNode(DummyItem, InvokeMethod("get_price"))
    n2 = OpNode(DummyItem, InvokeMethod("get_price"))
    n3 = MergeNode()
    n4 = OpNode(DummyUser, InvokeMethod("buy_2_items_1"))
    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n0, n2))
    df.add_edge(Edge(n1, n3))
    df.add_edge(Edge(n2, n3))
    df.add_edge(Edge(n3, n4))

    # User with key "foo" buys items with keys "fork" and "spoon"
    event = Event(n0, ["foo"], ["fork", "spoon"], {}, df)

    # Propogate the event (without actually doing any calculation)
    # Normally, the key_stack should've been updated by the runtime here:
    key_stack = ["foo", ["fork", "spoon"]]
    next_event = event.propogate(key_stack, None, None)

    assert len(next_event) == 2
    assert isinstance(next_event[0].target, OpNode)
    assert isinstance(next_event[1].target, OpNode)
    assert next_event[0].target.cls == DummyItem
    assert next_event[1].target.cls == DummyItem

    event1, event2 = next_event
    next_event = event1.propogate(event1.key_stack, None, None)
    assert len(next_event) == 1
    assert isinstance(next_event[0].target, MergeNode)

    next_event = event2.propogate(event2.key_stack, None, None)
    assert len(next_event) == 1
    assert isinstance(next_event[0].target, MergeNode)
    
    final_event = next_event[0].propogate(next_event[0].key_stack, None, None)
    assert final_event[0].target == n4
