from typing import Any
from cascade.dataflow.dataflow import CollectNode, DataFlow, Edge, InvokeMethod, OpNode, SelectAllNode
from cascade.dataflow.operator import StatelessOperator
from deathstar.entities.hotel import Geo, Hotel, hotel_op

# Stateless
class Search():
    # Get the 5 nearest hotels
    @staticmethod
    def nearby(lat: float, lon: float, in_date: int, out_date: int):
        distances = [
            (dist, hotel) 
                    for hotel in Hotel.__all__() 
                    if (dist := hotel.geo.distance_km(lat, lon)) < 10]
        hotels = [hotel for dist, hotel in sorted(distances)[:5]]
        return hotels


#### COMPILED FUNCTIONS (ORACLE) #####

    

# predicate 1
def search_nearby_compiled_0(variable_map: dict[str, Any], key_stack: list[str]):
    # We assume that the top of the key stack is the hotel key.
    # This assumption holds if the node before this one is a correctly
    # configure SelectAllNode.

    hotel_key = key_stack[-1]
    # The body will need the hotel key (actually, couldn't we just take the top of the key stack again?)
    variable_map["hotel_key"] = hotel_key
    # The next node (Hotel.get_geo) will need the hotel key
    key_stack.append(hotel_key)
    
# predicate 2
def search_nearby_compiled_1(variable_map: dict[str, Any], key_stack: list[str]):
    hotel_geo: Geo = variable_map["hotel_geo"]
    lat, lon = variable_map["lat"], variable_map["lon"]
    dist = hotel_geo.distance_km(lat, lon)
    variable_map["dist"] = dist
    return dist < 10


# body
def search_nearby_compiled_2(variable_map: dict[str, Any], key_stack: list[str]):
    return (variable_map["dist"], variable_map["hotel_key"])

# next line
def search_nearby_compiled_3(variable_map: dict[str, Any], key_stack: list[str]):
    distances = variable_map["distances"]
    hotels = [hotel for dist, hotel in sorted(distances)[:5]]
    return hotels
    

search_op = StatelessOperator({
    "search_nearby_compiled_0": search_nearby_compiled_0,
    "search_nearby_compiled_1": search_nearby_compiled_1,
    "search_nearby_compiled_2": search_nearby_compiled_2,
    "search_nearby_compiled_3": search_nearby_compiled_3,
}, None)

df = DataFlow("search_nearby")
n1 = OpNode(search_op, InvokeMethod("search_nearby_compiled_0"))
n2 = OpNode(hotel_op, InvokeMethod("get_geo"), assign_result_to="hotel_geo")
n3 = OpNode(search_op, InvokeMethod("search_nearby_compiled_1"), is_conditional=True)
n4 = OpNode(search_op, InvokeMethod("search_nearby_compiled_2"), assign_result_to="search_body")
n5 = CollectNode("distances", "search_body")
n0 = SelectAllNode(Hotel, n5)

n6 = OpNode(search_op, InvokeMethod("search_nearby_compiled_3"))

df.add_edge(Edge(n0, n1))
df.add_edge(Edge(n1, n2))
df.add_edge(Edge(n2, n3))

# if true make the body
df.add_edge(Edge(n3, n4, if_conditional=True))
df.add_edge(Edge(n4, n5))
# if false skip past
df.add_edge(Edge(n3, n5, if_conditional=False))

df.add_edge(Edge(n5, n6))

df.entry = n0
search_op.dataflow = df