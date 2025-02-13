from typing import Any
from cascade.dataflow.dataflow import CollectNode, CollectTarget, DataFlow, Edge, Event, EventResult, InvokeMethod, OpNode
from cascade.dataflow.operator import StatefulOperator

class DummyUser:
    def __init__(self, key: str, balance: int):
        self.key: str = key
        self.balance: int = balance
    
    def buy_item(self, item: 'DummyItem') -> bool:
        item_price = item.get_price() # SSA
        self.balance -= item_price
        return self.balance >= 0

def buy_item_0_compiled(variable_map: dict[str, Any], state: DummyUser):
    return 

def buy_item_1_compiled(variable_map: dict[str, Any], state: DummyUser):
    state.balance -= variable_map["item_price"]
    return state.balance >= 0

class DummyItem:
    def __init__(self, key: str, price: int):
        self.key: str = key
        self.price: int = price

    def get_price(self) -> int:
        return self.price
    
def get_price_compiled(variable_map: dict[str, Any], state: DummyItem):
    return state.price

################## TESTS #######################

user = DummyUser("user", 100)
item = DummyItem("fork", 5)

user_sop = StatefulOperator(DummyUser, 
                            {"buy_item_0": buy_item_0_compiled,
                             "buy_item_1": buy_item_1_compiled}, None)


def test_simple_df_propogation():
    df = DataFlow("user.buy_item")
    n1 = OpNode(DummyUser, InvokeMethod("buy_item_0_compiled"), read_key_from="user_key")
    n2 = OpNode(DummyItem, InvokeMethod("get_price"), read_key_from="item_key", assign_result_to="item_price")
    n3 = OpNode(DummyUser, InvokeMethod("buy_item_1"), read_key_from="user_key")
    df.add_edge(Edge(n1, n2))
    df.add_edge(Edge(n2, n3))

    user.buy_item(item)
    event = Event(n1, {"user_key": "user", "item_key":"fork"}, df)

    # Manually propogate
    item_key = buy_item_0_compiled(event.variable_map, state=user)
    next_event = event.propogate(event, item_key)

    assert isinstance(next_event, list)
    assert len(next_event) == 1
    assert next_event[0].target == n2
    event = next_event[0]

    # manually add the price to the variable map
    item_price = get_price_compiled(event.variable_map, state=item)
    assert n2.assign_result_to
    event.variable_map[n2.assign_result_to] = item_price

    next_event = event.propogate(item_price)

    assert isinstance(next_event, list)
    assert len(next_event) == 1
    assert next_event[0].target == n3
    event = next_event[0]

    positive_balance = buy_item_1_compiled(event.variable_map, state=user)
    next_event = event.propogate(None)
    assert isinstance(next_event, EventResult)
    

def test_merge_df_propogation():
    df = DataFlow("user.buy_2_items")
    n0 = OpNode(DummyUser, InvokeMethod("buy_2_items_0"), read_key_from="user_key")
    n3 = CollectNode(assign_result_to="item_prices", read_results_from="item_price")
    n1 = OpNode(
        DummyItem, 
        InvokeMethod("get_price"), 
        assign_result_to="item_price", 
        collect_target=CollectTarget(n3, 2, 0),
        read_key_from="item_1_key"
    )
    n2 = OpNode(
        DummyItem, 
        InvokeMethod("get_price"), 
        assign_result_to="item_price", 
        collect_target=CollectTarget(n3, 2, 1),
        read_key_from="item_2_key"
    )
    n4 = OpNode(DummyUser, InvokeMethod("buy_2_items_1"), read_key_from="user_key")
    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n0, n2))
    df.add_edge(Edge(n1, n3))
    df.add_edge(Edge(n2, n3))
    df.add_edge(Edge(n3, n4))

    # User with key "foo" buys items with keys "fork" and "spoon"
    event = Event(n0, {"user_key": "foo", "item_1_key": "fork", "item_2_key": "spoon"}, df)

    # Propogate the event (without actually doing any calculation)
    # Normally, the key_stack should've been updated by the runtime here:
    next_event = event.propogate(None)

    assert isinstance(next_event, list)
    assert len(next_event) == 2
    assert next_event[0].target == n1
    assert next_event[1].target == n2

    event1, event2 = next_event
    next_event = event1.propogate(None)

    assert isinstance(next_event, list)
    assert len(next_event) == 1
    assert next_event[0].target == n3

    next_event = event2.propogate(None)

    assert isinstance(next_event, list)
    assert len(next_event) == 1
    assert next_event[0].target == n3
    
    final_event = next_event[0].propogate(None)
    assert isinstance(final_event, list)
    assert final_event[0].target == n4
