from typing import Any

from cascade.dataflow.dataflow import CallEntity, CallLocal, DataFlow, Edge, InvokeMethod, OpNode
from test_programs.target.checkout_item import User, Item

def buy_item_0_compiled(variable_map: dict[str, Any], state: User) -> Any:
    return None


def buy_item_1_compiled(variable_map: dict[str, Any], state: User) -> Any:
    state.balance -= variable_map['item_price_0']
    return state.balance >= 0


def get_price_0_compiled(variable_map: dict[str, Any], state: Item) -> Any:
    return state.price

def item_get_price_df():
    df = DataFlow("item.get_price")
    n0 = CallLocal(InvokeMethod("get_price_0_compiled"))
    df.entry = n0
    return df

def user_buy_item_df():
    df = DataFlow("user.buy_item")
    n0 = CallLocal(InvokeMethod("buy_item_0_compiled"))
    n1 = CallEntity(item_get_price_df(), {}, "item_price_0")
    n2 = CallLocal(InvokeMethod("buy_item_1_compiled"))
    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n1, n2))
    df.entry = n0
    return df

