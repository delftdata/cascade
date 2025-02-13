from typing import Any

from cascade.dataflow.dataflow import DataFlow, Edge, InvokeMethod, OpNode
from test_programs.target.checkout_item import User, Item

def buy_item_0_compiled(variable_map: dict[str, Any], state: User) -> Any:
    return None


def buy_item_1_compiled(variable_map: dict[str, Any], state: User) -> Any:
    item_price_0 = variable_map['item_price_0']
    state.balance -= item_price_0
    return state.balance >= 0


def get_price_0_compiled(variable_map: dict[str, Any], state: Item) -> Any:
    return state.price


def user_buy_item_df():
    df = DataFlow("user.buy_item")
    n0 = OpNode(User, InvokeMethod("buy_item_0"), read_key_from="user_key")
    n1 = OpNode(Item, InvokeMethod("get_price"), assign_result_to="item_price", read_key_from="item_key")
    n2 = OpNode(User, InvokeMethod("buy_item_1"), read_key_from="user_key")
    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n1, n2))
    df.entry = n0
    return df

