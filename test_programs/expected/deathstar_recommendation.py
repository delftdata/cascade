from typing import Any, Literal
from cascade.dataflow.dataflow import CollectNode, DataFlow, Edge, InvokeMethod, OpNode, SelectAllNode
from cascade.dataflow.operator import StatelessOperator


def get_recs_if_cond(variable_map: dict[str, Any], key_stack: list[str]):
    return variable_map["requirement"] == "distance"

# list comprehension entry
def get_recs_if_body_0(variable_map: dict[str, Any], key_stack: list[str]):
    hotel_key = key_stack[-1]
    # The body will need the hotel key (actually, couldn't we just take the top of the key stack again?)
    variable_map["hotel_key"] = hotel_key
    # The next node (Hotel.get_geo) will need the hotel key
    key_stack.append(hotel_key)


# list comprehension body
def get_recs_if_body_1(variable_map: dict[str, Any], key_stack: list[str]):
    hotel_geo: Geo = variable_map["hotel_geo"]
    lat, lon = variable_map["lat"], variable_map["lon"]
    dist = hotel_geo.distance_km(lat, lon)
    return (dist, variable_map["hotel_key"])

# after list comprehension
def get_recs_if_body_2(variable_map: dict[str, Any], key_stack: list[str]):
    distances = variable_map["distances"]
    min_dist = min(distances, key=lambda x: x[0])[0]
    variable_map["res"] = [hotel for dist, hotel in distances if dist == min_dist]


def get_recs_elif_cond(variable_map: dict[str, Any], key_stack: list[str]):
    return variable_map["requirement"] == "price"


# list comprehension entry
def get_recs_elif_body_0(variable_map: dict[str, Any], key_stack: list[str]):
    hotel_key = key_stack[-1]
    # The body will need the hotel key (actually, couldn't we just take the top of the key stack again?)
    variable_map["hotel_key"] = hotel_key
    # The next node (Hotel.get_geo) will need the hotel key
    key_stack.append(hotel_key)


# list comprehension body
def get_recs_elif_body_1(variable_map: dict[str, Any], key_stack: list[str]):
    return (variable_map["hotel_price"], variable_map["hotel_key"])

# after list comprehension
def get_recs_elif_body_2(variable_map: dict[str, Any], key_stack: list[str]):
    prices = variable_map["prices"]
    min_price = min(prices, key=lambda x: x[0])[0]
    variable_map["res"] = [hotel for price, hotel in prices if price == min_price]



# a future optimization might instead duplicate this piece of code over the two 
# branches, in order to reduce the number of splits by one
def get_recs_final(variable_map: dict[str, Any], key_stack: list[str]):
    return variable_map["res"]


recommend_op = StatelessOperator({
    "get_recs_if_cond": get_recs_if_cond,
    "get_recs_if_body_0": get_recs_if_body_0,
    "get_recs_if_body_1": get_recs_if_body_1,
    "get_recs_if_body_2": get_recs_if_body_2,
    "get_recs_elif_cond": get_recs_elif_cond,
    "get_recs_elif_body_0": get_recs_elif_body_0,
    "get_recs_elif_body_1": get_recs_elif_body_1,
    "get_recs_elif_body_2": get_recs_elif_body_2,
    "get_recs_final": get_recs_final,
}, None)

def get_recommendations_df():
    df = DataFlow("get_recommendations")
    n1 = OpNode(recommend_op, InvokeMethod("get_recs_if_cond"), is_conditional=True)
    n2 = OpNode(recommend_op, InvokeMethod("get_recs_if_body_0"))
    n3 = OpNode(hotel_op, InvokeMethod("get_geo"), assign_result_to="hotel_geo")
    n4 = OpNode(recommend_op, InvokeMethod("get_recs_if_body_1"), assign_result_to="distance")
    n5 = CollectNode("distances", "distance")
    n6 = OpNode(recommend_op, InvokeMethod("get_recs_if_body_2"))
    ns1 = SelectAllNode(Hotel, n5)

    n7 = OpNode(recommend_op, InvokeMethod("get_recs_elif_cond"), is_conditional=True)
    n8 = OpNode(recommend_op, InvokeMethod("get_recs_elif_body_0"))
    n9 = OpNode(hotel_op, InvokeMethod("get_price"), assign_result_to="hotel_price")
    n10 = OpNode(recommend_op, InvokeMethod("get_recs_elif_body_1"), assign_result_to="price")
    n11 = CollectNode("prices", "price")
    n12 = OpNode(recommend_op, InvokeMethod("get_recs_elif_body_2"))
    ns2 = SelectAllNode(Hotel, n11)


    n13 = OpNode(recommend_op, InvokeMethod("get_recs_final"))

    df.add_edge(Edge(n1, ns1, if_conditional=True))
    df.add_edge(Edge(n1, n7, if_conditional=False))
    df.add_edge(Edge(n7, ns2, if_conditional=True))
    df.add_edge(Edge(n7, n13, if_conditional=False))

    # if branch
    df.add_edge(Edge(ns1, n2))
    df.add_edge(Edge(n2, n3))
    df.add_edge(Edge(n3, n4))
    df.add_edge(Edge(n4, n5))
    df.add_edge(Edge(n5, n6))
    df.add_edge(Edge(n6, n13))

    # elif branch
    df.add_edge(Edge(ns2, n8))
    df.add_edge(Edge(n8, n9))
    df.add_edge(Edge(n9, n10))
    df.add_edge(Edge(n10, n11))
    df.add_edge(Edge(n11, n12))
    df.add_edge(Edge(n12, n13))

    df.entry = n1
    return df

recommend_op.dataflow = get_recommendations_df()