from typing import Any
from cascade.dataflow.dataflow import CollectNode, CollectTarget, DataFlow, Edge, InvokeMethod, OpNode
from cascade.dataflow.operator import StatefulOperator
from deathstar_hotel_reservation.entities.flight import Flight, flight_op
from deathstar_hotel_reservation.entities.hotel import Hotel, hotel_op


class User():
    def __init__(self, user_id: str, password: str):
        self.id = user_id
        self.password = password

    def check(self, password):
        return self.password == password
    
    def order(self, flight: Flight, hotel: Hotel):
        if hotel.reserve() and flight.reserve():
            return True
        else:
            return False
        
#### COMPILED FUNCTIONS (ORACLE) #####

def check_compiled(variable_map: dict[str, Any], state: User) -> Any:
    return state.password == variable_map["password"]

def order_compiled_entry_0(variable_map: dict[str, Any], state: User) -> Any:
    pass

def order_compiled_entry_1(variable_map: dict[str, Any], state: User) -> Any:
    pass

def order_compiled_if_cond(variable_map: dict[str, Any], state: User) -> Any:
    # parallel
    if "reserves" in variable_map:
        return variable_map["reserves"][0] and variable_map["reserves"][1]
    else:
        return variable_map["hotel_reserve"] and variable_map["flight_reserve"]

def order_compiled_if_body(variable_map: dict[str, Any], state: User) -> Any:
    return True

def order_compiled_else_body(variable_map: dict[str, Any], state: User) -> Any:
    return False

user_op = StatefulOperator(
    User,
    {
        "login": check_compiled,
        "order_compiled_entry_0": order_compiled_entry_0,
        "order_compiled_entry_1": order_compiled_entry_1,
        "order_compiled_if_cond": order_compiled_if_cond,
        "order_compiled_if_body": order_compiled_if_body,
        "order_compiled_else_body": order_compiled_else_body
    },
    {}
)

# For now, the dataflow will be serial instead of parallel. Future optimizations
# will try to automatically parallelize this.
# There is also no user entry (this could also be an optimization)
def df_serial():
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


# PARALLEL DATAFLOW
def df_parallel():
    df = DataFlow("user_order")
    n0 = OpNode(User, InvokeMethod("order_compiled_entry_0"), read_key_from="user_key")
    ct = CollectNode(assign_result_to="reserves", read_results_from="reserve")
    n1 = OpNode(Hotel, InvokeMethod("reserve"), assign_result_to="reserve", read_key_from="hotel_key", collect_target=CollectTarget(ct, 2, 0))
    n3 = OpNode(Flight, InvokeMethod("reserve"), assign_result_to="reserve", read_key_from="flight_key", collect_target=CollectTarget(ct, 2, 1))
    n4 = OpNode(User, InvokeMethod("order_compiled_if_cond"), is_conditional=True, read_key_from="user_key")
    n5 = OpNode(User, InvokeMethod("order_compiled_if_body"), read_key_from="user_key")
    n6 = OpNode(User, InvokeMethod("order_compiled_else_body"), read_key_from="user_key")

    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n0, n3))
    df.add_edge(Edge(n1, ct))
    df.add_edge(Edge(n3, ct))
    df.add_edge(Edge(ct, n4))
    df.add_edge(Edge(n4, n5, if_conditional=True))
    df.add_edge(Edge(n4, n6, if_conditional=False))

    df.entry = n0
    return df

user_op.dataflows["order"] = df_serial()
