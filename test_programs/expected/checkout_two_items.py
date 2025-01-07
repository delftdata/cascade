from typing import Any
# from ..target.checkout_item import User, Item
# from cascade.dataflow.dataflow import DataFlow, OpNode, InvokeMethod, Edge

def buy_two_items_0_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.append(variable_map['item_1_key'])
    key_stack.append(variable_map['item_2_key'])
    return None


def buy_two_items_1_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.pop()
    item_price_2_0 = variable_map['item_price_2_0']
    item_price_1_0 = variable_map['item_price_1_0']
    total_price_0 = item_price_1_0 + item_price_2_0
    state.balance -= total_price_0
    return state.balance >= 0


def get_price_0_compiled(variable_map: dict[str, Any], state: Item, key_stack: list[str]) -> Any:
    key_stack.pop()
    return state.price


def user_buy_item_df():
    df = DataFlow("user.buy_item")
    n0 = OpNode(User, InvokeMethod("buy_item_0"))
    n1 = OpNode(Item, InvokeMethod("get_price"), assign_result_to="item_price")
    n2 = OpNode(User, InvokeMethod("buy_item_1"))
    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n1, n2))
    df.entry = n0
    return df

