from typing import Any

from cascade.dataflow.dataflow import CollectNode, DataFlow, Edge, InvokeMethod, OpNode, SelectAllNode, StatelessOpNode
from cascade.dataflow.operator import StatelessOperator

# predicate 1
def search_nearby_compiled_0(variable_map: dict[str, Any]):
    pass
    
# predicate 2
def search_nearby_compiled_1(variable_map: dict[str, Any]):
    hotel_geo: Geo = variable_map["hotel_geo"]
    lat, lon = variable_map["lat"], variable_map["lon"]
    dist = hotel_geo.distance_km(lat, lon)
    variable_map["dist"] = dist
    return dist < 10


# body
def search_nearby_compiled_2(variable_map: dict[str, Any]):
    return (variable_map["dist"], variable_map["hotel_key"])

# next line
def search_nearby_compiled_3(variable_map: dict[str, Any]):
    distances = variable_map["distances"]
    hotels = [hotel for dist, hotel in sorted(distances)[:5]]
    return hotels
    

search_op = StatelessOperator({
    "search_nearby_compiled_0": search_nearby_compiled_0,
    "search_nearby_compiled_1": search_nearby_compiled_1,
    "search_nearby_compiled_2": search_nearby_compiled_2,
    "search_nearby_compiled_3": search_nearby_compiled_3,
}, None)

def search_nearby_df():
    df = DataFlow("search_nearby")
    n1 = StatelessOpNode(search_op, InvokeMethod("search_nearby_compiled_0"))
    n2 = OpNode(Hotel, InvokeMethod("get_geo"), assign_result_to="hotel_geo", read_key_from="hotel_key")
    n3 = StatelessOpNode(search_op, InvokeMethod("search_nearby_compiled_1"), is_conditional=True)
    n4 = StatelessOpNode(search_op, InvokeMethod("search_nearby_compiled_2"), assign_result_to="search_body")
    n5 = CollectNode("distances", "search_body")
    n0 = SelectAllNode(Hotel, n5, assign_key_to="hotel_key")

    n6 = StatelessOpNode(search_op, InvokeMethod("search_nearby_compiled_3"))

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
    return df

search_op.dataflow = search_nearby_df()