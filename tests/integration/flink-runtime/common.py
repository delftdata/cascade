from typing import Any
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

def update_balance_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.pop() # final function
    state.balance += variable_map["amount"]
    return state.balance >= 0

def get_balance_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.pop() # final function
    return state.balance

def get_price_compiled(variable_map: dict[str, Any], state: Item, key_stack: list[str]) -> Any:
    key_stack.pop() # final function
    variable_map["item_price"] = state.price
    return state.price

# Items (or other operators) are passed by key always
def buy_item_0_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.append(variable_map["item_key"])
    return 

def buy_item_1_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.pop()
    state.balance -= variable_map["item_price"]
    return state.balance >= 0


def buy_2_items_0_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.append(
        [variable_map["item1_key"], variable_map["item2_key"]]
    )
    return None

def buy_2_items_1_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.pop()
    state.balance -= variable_map["item1_price"] + variable_map["item2_price"]
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
    })

item_op = StatefulOperator(
    Item, {"get_price": get_price_compiled}
)
