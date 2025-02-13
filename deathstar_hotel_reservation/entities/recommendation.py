from typing import Any, Literal
from cascade.dataflow.dataflow import CollectNode, DataFlow, Edge, InvokeMethod, OpNode, SelectAllNode, StatelessOpNode
from cascade.dataflow.operator import StatelessOperator
from deathstar_hotel_reservation.entities.hotel import Geo, Hotel

# Stateless
class Recommendation():
    @staticmethod
    def get_recommendations(requirement: Literal["distance", "price"], lat: float, lon: float) -> list[Hotel]:
        if requirement == "distance":
            distances = [(hotel.geo.distance_km(lat, lon), hotel)
                            for hotel in Hotel.__all__()]
            min_dist = min(distances, key=lambda x: x[0])
            res = [hotel for dist, hotel in distances if dist == min_dist]
        elif requirement == "price":
            prices = [(hotel.price, hotel)
                            for hotel in Hotel.__all__()]
            min_price = min(prices, key=lambda x: x[0])
            res = [hotel for rate, hotel in prices if rate == min_price]

        # todo: raise error on else ...?
        return res
    
#### COMPILED FUNCTIONS (ORACLE) #### 

def get_recs_if_cond(variable_map: dict[str, Any]):
    return variable_map["requirement"] == "distance"

# list comprehension entry
def get_recs_if_body_0(variable_map: dict[str, Any]):
    pass


# list comprehension body
def get_recs_if_body_1(variable_map: dict[str, Any]):
    hotel_geo: Geo = variable_map["hotel_geo"]
    lat, lon = variable_map["lat"], variable_map["lon"]
    dist = hotel_geo.distance_km(lat, lon)
    return (dist, variable_map["hotel_key"])

# after list comprehension
def get_recs_if_body_2(variable_map: dict[str, Any]):
    distances = variable_map["distances"]
    min_dist = min(distances, key=lambda x: x[0])[0]
    variable_map["res"] = [hotel for dist, hotel in distances if dist == min_dist]


def get_recs_elif_cond(variable_map: dict[str, Any]):
    return variable_map["requirement"] == "price"


# list comprehension entry
def get_recs_elif_body_0(variable_map: dict[str, Any]):
    pass


# list comprehension body
def get_recs_elif_body_1(variable_map: dict[str, Any]):
    return (variable_map["hotel_price"], variable_map["hotel_key"])

# after list comprehension
def get_recs_elif_body_2(variable_map: dict[str, Any]):
    prices = variable_map["prices"]
    min_price = min(prices, key=lambda x: x[0])[0]
    variable_map["res"] = [hotel for price, hotel in prices if price == min_price]



# a future optimization might instead duplicate this piece of code over the two 
# branches, in order to reduce the number of splits by one
def get_recs_final(variable_map: dict[str, Any]):
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

df = DataFlow("get_recommendations")
n1 = StatelessOpNode(recommend_op, InvokeMethod("get_recs_if_cond"), is_conditional=True)
n2 = StatelessOpNode(recommend_op, InvokeMethod("get_recs_if_body_0"))
n3 = OpNode(Hotel, InvokeMethod("get_geo"), assign_result_to="hotel_geo", read_key_from="hotel_key")
n4 = StatelessOpNode(recommend_op, InvokeMethod("get_recs_if_body_1"), assign_result_to="distance")
n5 = CollectNode("distances", "distance")
n6 = StatelessOpNode(recommend_op, InvokeMethod("get_recs_if_body_2"))
ns1 = SelectAllNode(Hotel, n5, assign_key_to="hotel_key")

n7 = StatelessOpNode(recommend_op, InvokeMethod("get_recs_elif_cond"), is_conditional=True)
n8 = StatelessOpNode(recommend_op, InvokeMethod("get_recs_elif_body_0"))
n9 = OpNode(Hotel, InvokeMethod("get_price"), assign_result_to="hotel_price", read_key_from="hotel_key")
n10 = StatelessOpNode(recommend_op, InvokeMethod("get_recs_elif_body_1"), assign_result_to="price")
n11 = CollectNode("prices", "price")
n12 = StatelessOpNode(recommend_op, InvokeMethod("get_recs_elif_body_2"))
ns2 = SelectAllNode(Hotel, n11, assign_key_to="hotel_key")


n13 = StatelessOpNode(recommend_op, InvokeMethod("get_recs_final"))

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
recommend_op.dataflow = df