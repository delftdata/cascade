"""
Basically we need a way to search through all state.
"""
import math
import random
from dataclasses import dataclass
from typing import Any

from pyflink.datastream.data_stream import CloseableIterator

from cascade.dataflow.dataflow import CollectNode, DataFlow, Edge, Event, EventResult, Filter, InitClass, InvokeMethod, MergeNode, OpNode, SelectAllNode
from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from cascade.runtime.flink_runtime import FlinkOperator, FlinkRuntime, FlinkStatelessOperator
from confluent_kafka import Producer
import time
import pytest

@dataclass
class Geo:
    x: int
    y: int

class Hotel:
    def __init__(self, name: str, loc: Geo):
        self.name = name
        self.loc = loc

    def get_name(self) -> str:
        return self.name
    
    def distance(self, loc: Geo) -> float:
        return math.sqrt((self.loc.x - loc.x) ** 2 + (self.loc.y - loc.y) ** 2)
    
    def __repr__(self) -> str:
        return f"Hotel({self.name}, {self.loc})"


def distance_compiled(variable_map: dict[str, Any], state: Hotel, key_stack: list[str]) -> Any:
    key_stack.pop()
    loc = variable_map["loc"]
    return math.sqrt((state.loc.x - loc.x) ** 2 + (state.loc.y - loc.y) ** 2)

def get_name_compiled(variable_map: dict[str, Any], state: Hotel, key_stack: list[str]) -> Any:
    key_stack.pop()
    return state.name

hotel_op = StatefulOperator(Hotel, 
                            {"distance": distance_compiled,
                             "get_name": get_name_compiled}, {})



def get_nearby(hotels: list[Hotel], loc: Geo, dist: float):
    return [hotel.get_name() for hotel in hotels if hotel.distance(loc) < dist]


# We compile just the predicate, the select is implemented using a selectall node
def get_nearby_predicate_compiled_0(variable_map: dict[str, Any], key_stack: list[str]):
    # the top of the key_stack is already the right key, so in this case we don't need to do anything
    # loc = variable_map["loc"]
    # we need the hotel_key for later. (body_compiled_0)
    variable_map["hotel_key"] = key_stack[-1]
    pass

def get_nearby_predicate_compiled_1(variable_map: dict[str, Any], key_stack: list[str]) -> bool:
    loc = variable_map["loc"]
    dist = variable_map["dist"]
    hotel_dist = variable_map["hotel_distance"]
    # key_stack.pop() # shouldn't pop because this function is stateless
    return hotel_dist < dist

def get_nearby_body_compiled_0(variable_map: dict[str, Any], key_stack: list[str]):
    key_stack.append(variable_map["hotel_key"])

def get_nearby_body_compiled_1(variable_map: dict[str, Any], key_stack: list[str]) -> str:
    return variable_map["hotel_name"]

get_nearby_op = StatelessOperator({
    "get_nearby_predicate_compiled_0": get_nearby_predicate_compiled_0,
    "get_nearby_predicate_compiled_1": get_nearby_predicate_compiled_1,
    "get_nearby_body_compiled_0": get_nearby_body_compiled_0,
    "get_nearby_body_compiled_1": get_nearby_body_compiled_1,
}, None)

# dataflow for getting all hotels within region
df = DataFlow("get_nearby")
n7 = CollectNode("get_nearby_result", "get_nearby_body")
n0 = SelectAllNode(Hotel, n7)
n1 = OpNode(get_nearby_op, InvokeMethod("get_nearby_predicate_compiled_0"))
n2 = OpNode(hotel_op, InvokeMethod("distance"), assign_result_to="hotel_distance")
n3 = OpNode(get_nearby_op, InvokeMethod("get_nearby_predicate_compiled_1"), is_conditional=True)
n4 = OpNode(get_nearby_op, InvokeMethod("get_nearby_body_compiled_0"))
n5 = OpNode(hotel_op, InvokeMethod("get_name"), assign_result_to="hotel_name")
n6 = OpNode(get_nearby_op, InvokeMethod("get_nearby_body_compiled_1"), assign_result_to="get_nearby_body")

df.add_edge(Edge(n0, n1))
df.add_edge(Edge(n1, n2))
df.add_edge(Edge(n2, n3))
df.add_edge(Edge(n3, n4, if_conditional=True))
df.add_edge(Edge(n3, n7, if_conditional=False))
df.add_edge(Edge(n4, n5))
df.add_edge(Edge(n5, n6))
df.add_edge(Edge(n6, n7))
get_nearby_op.dataflow = df

@pytest.mark.integration
def test_nearby_hotels():
    runtime = FlinkRuntime("test_nearby_hotels")
    runtime.init()
    runtime.add_operator(FlinkOperator(hotel_op))
    runtime.add_stateless_operator(FlinkStatelessOperator(get_nearby_op))

    # Create Hotels
    hotels = []
    init_hotel = OpNode(hotel_op, InitClass())
    random.seed(42)
    for i in range(20):
        coord_x = random.randint(-10, 10)
        coord_y = random.randint(-10, 10)
        hotel = Hotel(f"h_{i}", Geo(coord_x, coord_y))
        event = Event(init_hotel, [hotel.name], {"name": hotel.name, "loc": hotel.loc}, None) 
        runtime.send(event)
        hotels.append(hotel)

    collected_iterator: CloseableIterator = runtime.run(run_async=True, collect=True)
    records = []
    def wait_for_event_id(id: int) -> EventResult:
        for record in collected_iterator:
            records.append(record)
            print(f"Collected record: {record}")
            if record.event_id == id:
                return record
            
    def wait_for_n_records(num: int) -> list[EventResult]:
        i = 0
        n_records = []
        for record in collected_iterator:
            i += 1
            records.append(record)
            n_records.append(record)
            print(f"Collected record: {record}")
            if i == num:
                return n_records

    print("creating hotels")
    # Wait for hotels to be created
    wait_for_n_records(20)
    time.sleep(3) # wait for all hotels to be registered

    dist = 5
    loc = Geo(0, 0)
    # because of how the key stack works, we need to supply a key here
    event = Event(n0, ["workaround_key"], {"loc": loc, "dist": dist}, df) 
    runtime.send(event, flush=True)
    
    nearby = []
    for hotel in hotels:
        if hotel.distance(loc) < dist:
            nearby.append(hotel.name)

    event_result = wait_for_event_id(event._id)
    results = [r for r in event_result.result if r != None]
    print(nearby)
    assert set(results) == set(nearby)