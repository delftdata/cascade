from typing import Any
from cascade.dataflow.dataflow import DataFlow, Edge, InvokeMethod, OpNode
from cascade.dataflow.operator import StatefulOperator

def order_compiled_entry_0(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.append(variable_map["hotel"])

def order_compiled_entry_1(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.append(variable_map["flight"])

def order_compiled_if_cond(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    return variable_map["hotel_reserve"] and variable_map["flight_reserve"]

def order_compiled_if_body(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.pop()
    return True

def order_compiled_else_body(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
    key_stack.pop()
    return False


user_op = StatefulOperator(
    User,
    {
        "order_compiled_entry_0": order_compiled_entry_0,
        "order_compiled_entry_1": order_compiled_entry_1,
        "order_compiled_if_cond": order_compiled_if_cond,
        "order_compiled_if_body": order_compiled_if_body,
        "order_compiled_else_body": order_compiled_else_body
    },
    {} # dataflows (filled later)
)

# For now, the dataflow will be serial instead of parallel (calling hotel, then 
# flight). Future optimizations could try to automatically parallelize this.
# There could definetly be some slight changes to this dataflow depending on 
# other optimizations aswell. (A naive system could have an empty first entry
# before the first entity call).
def user_order_df():
    df = DataFlow("user_order")
    n0 = OpNode(user_op, InvokeMethod("order_compiled_entry_0"))
    n1 = OpNode(hotel_op, InvokeMethod("reserve"), assign_result_to="hotel_reserve")
    n2 = OpNode(user_op, InvokeMethod("order_compiled_entry_1"))
    n3 = OpNode(flight_op, InvokeMethod("reserve"), assign_result_to="flight_reserve")
    n4 = OpNode(user_op, InvokeMethod("order_compiled_if_cond"), is_conditional=True)
    n5 = OpNode(user_op, InvokeMethod("order_compiled_if_body"))
    n6 = OpNode(user_op, InvokeMethod("order_compiled_else_body"))

    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n1, n2))
    df.add_edge(Edge(n2, n3))
    df.add_edge(Edge(n3, n4))
    df.add_edge(Edge(n4, n5, if_conditional=True))
    df.add_edge(Edge(n4, n6, if_conditional=False))

    df.entry = n0
    return df

user_op.dataflows["order"] = user_order_df()
