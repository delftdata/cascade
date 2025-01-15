from typing import Any

def buy_item_0_compiled(variable_map: dict[str, Any], state, key_stack):
    key_stack.append(variable_map['item_key'])
    return None

def buy_item_1_compiled_if_cond(variable_map: dict[str, Any], state, key_stack):
    item_price_0 = variable_map['item_price_0']
    return item_price_0 < 100

def buy_item_compiled_if_body_0(variable_map: dict[str, Any], state, key_stack):
    key_stack.append('delivery_service_key')
    return None

def buy_item_compiled_if_body_1(variable_map: dict[str, Any], state, key_stack):
    delivery_costs_0 = variable_map['delivery_costs_0']
    item_price_0 = variable_map['item_price']
    total_price_0 = item_price_0 + delivery_costs_0
    return total_price_0

def buy_item_compiled_else_body_0(variable_map: dict[str, Any], state, key_stack):
    item_price_0 = variable_map['item_price_0']
    total_price_1 = item_price_0
    variable_map['total_price_1'] = total_price_1 # should this be return or adding to var map

def buy_item_2_compiled(variable_map: dict[str, Any], state, key_stack):
    total_price = variable_map['total_price']
    state.balance -= total_price_2
    return state.balance >= 0

