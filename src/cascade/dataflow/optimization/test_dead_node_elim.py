from typing import Any

from cascade.dataflow.dataflow import DataFlow, Edge, InvokeMethod
from cascade.dataflow.operator import StatefulOperator
from cascade.dataflow.optimization.dead_node_elim import dead_node_elimination
from cascade.dataflow.optimization.dead_node_elim import is_no_op

class User:
    pass

class Hotel:
    pass

class Flight:
    pass

def order_compiled_entry_0(variable_map: dict[str, Any], state: User) -> Any:
    pass

def order_compiled_entry_1(variable_map: dict[str, Any], state: User) -> Any:
    pass

def order_compiled_if_cond(variable_map: dict[str, Any], state: User) -> Any:
    return variable_map["hotel_reserve"] and variable_map["flight_reserve"]

def order_compiled_if_body(variable_map: dict[str, Any], state: User) -> Any:
    return True

def order_compiled_else_body(variable_map: dict[str, Any], state: User) -> Any:
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
    {}
)

# For now, the dataflow will be serial instead of parallel (calling hotel, then 
# flight). Future optimizations could try to automatically parallelize this.
# There could definetly be some slight changes to this dataflow depending on 
# other optimizations aswell. (A naive system could have an empty first entry
# before the first entity call).
def user_order_df():
    df = DataFlow("user_order")
    n0 = OpNode(User, InvokeMethod("order_compiled_entry_0"), read_key_from="user_key")
    n1 = OpNode(Hotel, InvokeMethod("reserve"), assign_result_to="hotel_reserve", read_key_from="hotel_key")
    n2 = OpNode(User, InvokeMethod("order_compiled_entry_1"), read_key_from="user_key")
    n3 = OpNode(Flight, InvokeMethod("reserve"), assign_result_to="flight_reserve", read_key_from="flight_key")
    n4 = OpNode(User, InvokeMethod("order_compiled_if_cond"), is_conditional=True, read_key_from="user_key")
    n5 = OpNode(User, InvokeMethod("order_compiled_if_body"), read_key_from="user_key")
    n6 = OpNode(User, InvokeMethod("order_compiled_else_body"), read_key_from="user_key")

    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n1, n2))
    df.add_edge(Edge(n2, n3))
    df.add_edge(Edge(n3, n4))
    df.add_edge(Edge(n4, n5, if_conditional=True))
    df.add_edge(Edge(n4, n6, if_conditional=False))

    df.entry = n0
    return df

# df = user_order_df()
# user_op.dataflows[df.name] = df

def DEPRECATED_test_dead_node_elim():
    print(user_op.dataflows[df.name].to_dot())

    dead_node_elimination([user_op], [])

    print(user_op.dataflows[df.name].to_dot())



### TEST NO OP DETECTION ###

def c1(variable_map: dict[str, Any]):
    return (variable_map["dist"], variable_map["hotel_key"])

def c2(variable_map: dict[str, Any]):
    return None

def c3(variable_map: dict[str, Any]):
    return

def c4(variable_map: dict[str, Any]):
    pass

def c5(variable_map: dict[str, Any]):
    return True

def test_no_op_detect():
    assert not is_no_op(c1)
    assert not is_no_op(c2)
    assert is_no_op(c3)
    assert is_no_op(c4)
    assert not is_no_op(c5)
