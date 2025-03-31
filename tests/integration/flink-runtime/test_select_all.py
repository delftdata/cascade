# """
# The select all operator is used to fetch all keys for a single entity
# """
# import math
# import random
# from dataclasses import dataclass
# from typing import Any

# from pyflink.datastream.data_stream import CloseableIterator

# from cascade.dataflow.dataflow import CollectNode, DataFlow, Edge, Event, EventResult,  InitClass, InvokeMethod, OpNode, SelectAllNode, StatelessOpNode
# from cascade.dataflow.operator import StatefulOperator, StatelessOperator
# from cascade.runtime.flink_runtime import FlinkOperator, FlinkRuntime, FlinkStatelessOperator
# import time
# import pytest

# @dataclass
# class Geo:
#     x: int
#     y: int

# class Hotel:
#     def __init__(self, name: str, loc: Geo):
#         self.name = name
#         self.loc = loc

#     def get_name(self) -> str:
#         return self.name
    
#     def distance(self, loc: Geo) -> float:
#         return math.sqrt((self.loc.x - loc.x) ** 2 + (self.loc.y - loc.y) ** 2)
    
#     def __repr__(self) -> str:
#         return f"Hotel({self.name}, {self.loc})"


# def distance_compiled(variable_map: dict[str, Any], state: Hotel) -> Any:
#     loc = variable_map["loc"]
#     return math.sqrt((state.loc.x - loc.x) ** 2 + (state.loc.y - loc.y) ** 2)

# def get_name_compiled(variable_map: dict[str, Any], state: Hotel) -> Any:
#     return state.name

# hotel_op = StatefulOperator(Hotel, 
#                             {"distance": distance_compiled,
#                              "get_name": get_name_compiled}, {})



# def get_nearby(hotels: list[Hotel], loc: Geo, dist: float):
#     return [hotel.get_name() for hotel in hotels if hotel.distance(loc) < dist]


# # We compile just the predicate, the select is implemented using a selectall node
# def get_nearby_predicate_compiled_0(variable_map: dict[str, Any]):
#     pass

# def get_nearby_predicate_compiled_1(variable_map: dict[str, Any]) -> bool:
#     loc = variable_map["loc"]
#     dist = variable_map["dist"]
#     hotel_dist = variable_map["hotel_distance"]
#     return hotel_dist < dist

# def get_nearby_body_compiled_0(variable_map: dict[str, Any]):
#     pass

# def get_nearby_body_compiled_1(variable_map: dict[str, Any]) -> str:
#     return variable_map["hotel_name"]

# get_nearby_op = StatelessOperator({
#     "get_nearby_predicate_compiled_0": get_nearby_predicate_compiled_0,
#     "get_nearby_predicate_compiled_1": get_nearby_predicate_compiled_1,
#     "get_nearby_body_compiled_0": get_nearby_body_compiled_0,
#     "get_nearby_body_compiled_1": get_nearby_body_compiled_1,
# }, None)

# # dataflow for getting all hotels within region
# df = DataFlow("get_nearby")
# n7 = CollectNode("get_nearby_result", "get_nearby_body")
# n0 = SelectAllNode(Hotel, n7, assign_key_to="hotel_key")
# n1 = StatelessOpNode(get_nearby_op, InvokeMethod("get_nearby_predicate_compiled_0"))
# n2 = OpNode(Hotel, InvokeMethod("distance"), assign_result_to="hotel_distance", read_key_from="hotel_key")
# n3 = StatelessOpNode(get_nearby_op, InvokeMethod("get_nearby_predicate_compiled_1"), is_conditional=True)
# n4 = StatelessOpNode(get_nearby_op, InvokeMethod("get_nearby_body_compiled_0"))
# n5 = OpNode(Hotel, InvokeMethod("get_name"), assign_result_to="hotel_name", read_key_from="hotel_key")
# n6 = StatelessOpNode(get_nearby_op, InvokeMethod("get_nearby_body_compiled_1"), assign_result_to="get_nearby_body")

# df.add_edge(Edge(n0, n1))
# df.add_edge(Edge(n1, n2))
# df.add_edge(Edge(n2, n3))
# df.add_edge(Edge(n3, n4, if_conditional=True))
# df.add_edge(Edge(n3, n7, if_conditional=False))
# df.add_edge(Edge(n4, n5))
# df.add_edge(Edge(n5, n6))
# df.add_edge(Edge(n6, n7))
# get_nearby_op.dataflow = df

# @pytest.mark.integration
# def test_nearby_hotels():
#     runtime = FlinkRuntime("test_nearby_hotels")
#     runtime.init()
#     runtime.add_operator(hotel_op)
#     runtime.add_stateless_operator(get_nearby_op)

#     # Create Hotels
#     hotels = []
#     init_hotel = OpNode(Hotel, InitClass(), read_key_from="name")
#     random.seed(42)
#     for i in range(20):
#         coord_x = random.randint(-10, 10)
#         coord_y = random.randint(-10, 10)
#         hotel = Hotel(f"h_{i}", Geo(coord_x, coord_y))
#         event = Event(init_hotel, {"name": hotel.name, "loc": hotel.loc}, None) 
#         runtime.send(event)
#         hotels.append(hotel)

#     collected_iterator: CloseableIterator = runtime.run(run_async=True, output='collect')
#     records = []
#     def wait_for_event_id(id: int) -> EventResult:
#         for record in collected_iterator:
#             records.append(record)
#             print(f"Collected record: {record}")
#             if record.event_id == id:
#                 return record
            
#     def wait_for_n_records(num: int) -> list[EventResult]:
#         i = 0
#         n_records = []
#         for record in collected_iterator:
#             i += 1
#             records.append(record)
#             n_records.append(record)
#             print(f"Collected record: {record}")
#             if i == num:
#                 return n_records

#     print("creating hotels")
#     # Wait for hotels to be created
#     wait_for_n_records(20)
#     time.sleep(10) # wait for all hotels to be registered

#     dist = 5
#     loc = Geo(0, 0)
#     event = Event(n0, {"loc": loc, "dist": dist}, df) 
#     runtime.send(event, flush=True)
    
#     nearby = []
#     for hotel in hotels:
#         if hotel.distance(loc) < dist:
#             nearby.append(hotel.name)

#     event_result = wait_for_event_id(event._id)
#     results = [r for r in event_result.result if r != None]
#     print(nearby)
#     assert set(results) == set(nearby)