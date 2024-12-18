"""
Basically we need a way to search through all state.
"""
import math
import random
from dataclasses import dataclass
from typing import Any, Type

from pyflink.common import Configuration
from pyflink.datastream import ProcessFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.data_stream import CloseableIterator

from cascade.dataflow.dataflow import DataFlow, Edge, Event, EventResult, Filter, InitClass, OpNode, SelectAllNode
from cascade.dataflow.operator import StatefulOperator
from cascade.runtime.flink_runtime import ByteSerializer, FlinkOperator, FlinkRegisterKeyNode, FlinkRuntime, FlinkSelectAllOperator
from confluent_kafka import Producer
import os
import cloudpickle # problems with pickling functions (e.g. lambdas)? use cloudcloudpickle
import logging
import time

def add_kafka_source(env: StreamExecutionEnvironment, topics, broker="localhost:9092"):
    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
        'bin/flink-sql-connector-kafka-3.3.0-1.20.jar')
    serializer_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'bin/flink-kafka-bytes-serializer.jar')

    if os.name == 'nt':
        env.add_jars(f"file:///{kafka_jar}",f"file://{serializer_jar}")
    else:
        env.add_jars(f"file://{kafka_jar}",f"file://{serializer_jar}")

    deserialization_schema = ByteSerializer()
    properties: dict = {
        "bootstrap.servers": broker,
        "auto.offset.reset": "earliest", 
        "group.id": "test_group_1",
    }
    kafka_consumer = FlinkKafkaConsumer(topics, deserialization_schema, properties)
    return env.add_source(kafka_consumer)

def dbg(e, msg=""):
    print(msg + str(e))
    return e

@dataclass
class Geo:
    x: int
    y: int

class Hotel:
    def __init__(self, name: str, loc: Geo):
        self.name = name
        self.loc = loc

    def distance(self, loc: Geo) -> float:
        return math.sqrt((self.loc.x - loc.x) ** 2 + (self.loc.y - loc.y) ** 2)
    
    def __repr__(self) -> str:
        return f"Hotel({self.name}, {self.loc})"


def distance_compiled(variable_map: dict[str, Any], state: Hotel, key_stack: list[str]) -> Any:
    loc = variable_map["loc"]
    return math.sqrt((state.loc.x - loc.x) ** 2 + (state.loc.y - loc.y) ** 2)



# We compile just the predicate, the select is implemented using a selectall node
def get_nearby_predicate_compiled(variable_map: dict[str, Any], state: Hotel) -> bool:
    return state.distance(variable_map["loc"]) < variable_map["dist"]

hotel_op = StatefulOperator(Hotel, {"distance": distance_compiled}, {})

def test_nearby_hotels():
    runtime = FlinkRuntime("test_nearby_hotels")
    runtime.init()
    runtime.add_operator(FlinkOperator(hotel_op))

    # Create Hotels
    hotels = []
    init_hotel = OpNode(Hotel, InitClass())
    random.seed(42)
    for i in range(50):
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

    # Wait for hotels to be created
    wait_for_event_id(event._id)

    # GET NEARBY
    # dataflow for getting all hotels within region
    df = DataFlow("get_nearby_hotels")
    n0 = SelectAllNode(Hotel)
    n1 = OpNode(Hotel, Filter(get_nearby_predicate_compiled))
    df.add_edge(Edge(n0, n1))

    dist = 5
    loc = Geo(0, 0)
    event = Event(n0, [], {"loc": loc, "dist": dist}, df) 
    runtime.send(event, flush=True)
    
    nearby = []
    for hotel in hotels:
        if hotel.distance(loc) < dist:
            nearby.append(hotel.name)

    sol = wait_for_event_id(event._id)
    print(nearby)
    print(sol)
    print(records)
    assert sol.result in nearby
