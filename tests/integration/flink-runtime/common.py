from typing import Any
from cascade.dataflow.dataflow import CollectNode, CollectTarget, DataFlow, Edge, InvokeMethod, OpNode
from cascade.runtime.flink_runtime import StatefulOperator

class User:
    def __init__(self, key: str, balance: int):
        self.key: str = key
        self.balance: int = balance

    def update_balance(self, amount: int) -> bool:
        self.balance += amount
        return self.balance >= 0
    
    def get_balance(self) -> int:
        return self.balance
    
    def buy_item(self, item: 'Item') -> bool:
        item_price = item.get_price() # SSA
        self.balance -= item_price
        return self.balance >= 0
    
    def buy_2_items(self, item1: 'Item', item2: 'Item') -> bool:
        item1_price = item1.get_price() # SSA
        item2_price = item2.get_price() # SSA
        self.balance -= item1_price + item2_price
        return self.balance >= 0
    
    def __repr__(self):
        return f"User(key='{self.key}', balance={self.balance})"
    
class Item:
    def __init__(self, key: str, price: int):
        self.key: str = key
        self.price: int = price

    def get_price(self) -> int:
        return self.price
    
    def __repr__(self):
        return f"Item(key='{self.key}', price={self.price})"

def update_balance_compiled(variable_map: dict[str, Any], state: User) -> Any:
    state.balance += variable_map["amount"]
    return state.balance >= 0

def get_balance_compiled(variable_map: dict[str, Any], state: User) -> Any:
    return state.balance

def get_price_compiled(variable_map: dict[str, Any], state: Item) -> Any:
    return state.price

def buy_item_0_compiled(variable_map: dict[str, Any], state: User) -> Any:
    return None

def buy_item_1_compiled(variable_map: dict[str, Any], state: User) -> Any:
    state.balance = state.balance - variable_map["item_price"]
    return state.balance >= 0


def buy_2_items_0_compiled(variable_map: dict[str, Any], state: User) -> Any:
    return None

def buy_2_items_1_compiled(variable_map: dict[str, Any], state: User) -> Any:
    state.balance -= variable_map["item_prices"][0] + variable_map["item_prices"][1]
    return state.balance >= 0

# An operator is defined by the underlying class and the functions that can be called
user_op = StatefulOperator(
    User, 
    {
        "update_balance": update_balance_compiled, 
        "get_balance": get_balance_compiled, 
        "buy_item_0": buy_item_0_compiled,
        "buy_item_1": buy_item_1_compiled,
        "buy_2_items_0": buy_2_items_0_compiled,
        "buy_2_items_1": buy_2_items_1_compiled
    }, 
   {})

item_op = StatefulOperator(
    Item, {"get_price": get_price_compiled}, {}
)


def user_buy_item_df():
    df = DataFlow("user.buy_item")
    n0 = OpNode(User, InvokeMethod("buy_item_0"), read_key_from="user_key")
    n1 = OpNode(Item, 
                InvokeMethod("get_price"), 
                assign_result_to="item_price", 
                read_key_from="item_key")
    n2 = OpNode(User, InvokeMethod("buy_item_1"), read_key_from="user_key")
    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n1, n2))
    df.entry = n0
    return df

def user_buy_2_items_df():
    df = DataFlow("user.buy_2_items")
    n0 = OpNode(User, InvokeMethod("buy_2_items_0"), read_key_from="user_key")
    n3 = CollectNode(assign_result_to="item_prices", read_results_from="item_price")
    n1 = OpNode(
        Item, 
        InvokeMethod("get_price"), 
        assign_result_to="item_price", 
        collect_target=CollectTarget(n3, 2, 0),
        read_key_from="item1_key"
    )
    n2 = OpNode(
        Item, 
        InvokeMethod("get_price"), 
        assign_result_to="item_price", 
        collect_target=CollectTarget(n3, 2, 1),
        read_key_from="item2_key"
    )
    n4 = OpNode(User, InvokeMethod("buy_2_items_1"), read_key_from="user_key")
    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n0, n2))
    df.add_edge(Edge(n1, n3))
    df.add_edge(Edge(n2, n3))
    df.add_edge(Edge(n3, n4))
    df.entry = n0
    return df

user_op.dataflows =  {
        "buy_2_items": user_buy_2_items_df(),
        "buy_item": user_buy_item_df()
    }