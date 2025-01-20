from typing import Any

from cascade.dataflow.operator import StatefulOperator
from ..target.checkout_two_items import User, Item
from cascade.dataflow.dataflow import DataFlow, OpNode, InvokeMethod, Edge, CollectNode, CollectTarget

def buy_two_items_0_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.append(
        [variable_map["item1_key"], variable_map["item2_key"]]
    )
    return None

def buy_two_items_1_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.pop()
    item_price_1_0 = variable_map['item_price_1']
    item_price_2_0 = variable_map['item_price_2']
    total_price_0 = item_price_1_0 + item_price_2_0
    state.balance -= total_price_0
    return state.balance >= 0


def get_price_0_compiled(variable_map: dict[str, Any], state: Item, key_stack: list[str]) -> Any:
    key_stack.pop()
    return state.price


# An operator is defined by the underlying class and the functions that can be called
user_op = StatefulOperator(
    User, 
    {
        "buy_two_items_0": buy_two_items_0_compiled,
        "buy_two_items_1": buy_two_items_1_compiled
    }, 
   None)

item_op = StatefulOperator(
    Item, {"get_price": get_price_0_compiled}, None
)

def user_buy_two_items_df():
    df = DataFlow("user.buy_2_items")
    n0 = OpNode(user_op, InvokeMethod("buy_2_items_0"))
    n1 = OpNode(
        item_op, 
        InvokeMethod("get_price"), 
        assign_result_to="item_price_1", 
    )
    n2 = OpNode(
        item_op, 
        InvokeMethod("get_price"), 
        assign_result_to="item_price_2", 
    )
    n3 = OpNode(user_op, InvokeMethod("buy_2_items_1"))
    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n0, n2))
    df.add_edge(Edge(n1, n2))
    df.add_edge(Edge(n2, n3))
    df.entry = n0
    return df


# For future optimizations (not used)
def user_buy_two_items_df_parallelized():
    df = DataFlow("user.buy_2_items")
    n0 = OpNode(user_op, InvokeMethod("buy_2_items_0"))
    n3 = CollectNode(assign_result_to="item_prices", read_results_from="item_price")
    n1 = OpNode(
        item_op, 
        InvokeMethod("get_price"), 
        assign_result_to="item_price", 
        collect_target=CollectTarget(n3, 2, 0)
    )
    n2 = OpNode(
        item_op, 
        InvokeMethod("get_price"), 
        assign_result_to="item_price", 
        collect_target=CollectTarget(n3, 2, 1)
    )
    n4 = OpNode(user_op, InvokeMethod("buy_2_items_1"))
    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n0, n2))
    df.add_edge(Edge(n1, n3))
    df.add_edge(Edge(n2, n3))
    df.add_edge(Edge(n3, n4))
    df.entry = n0
    return df

user_op.dataflows =  {
    "buy_two_items": user_buy_two_items_df(),
}