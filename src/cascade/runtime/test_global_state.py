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

from cascade.dataflow.dataflow import DataFlow, Edge, Event, EventResult, Filter, InitClass, OpNode, SelectAllNode
from cascade.dataflow.operator import StatefulOperator
from cascade.runtime.flink_runtime import ByteSerializer, FlinkOperator, SelectAllOperator
from confluent_kafka import Producer
import os
import pickle # problems with pickling functions (e.g. lambdas)? use cloudpickle
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

def dbg(e):
    # print(e)
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




def test_yeeter():

    hotel_op = StatefulOperator(Hotel, {"distance": distance_compiled}, {})
    hotel_op = FlinkOperator(hotel_op)

    hotels = []
    random.seed(42)
    for i in range(100):
        coord_x = random.randint(-10, 10)
        coord_y = random.randint(-10, 10)
        hotels.append(Hotel(f"h_{i}", Geo(coord_x, coord_y)))

    def get_nearby(loc: Geo, dist: int) -> list[Hotel]:
        return [hotel for hotel in hotels if hotel.distance(loc) < dist]

    # Configure the local Flink instance with the ui at http://localhost:8081
    config = Configuration() # type: ignore
    config.set_string("rest.port", "8081")
    env = StreamExecutionEnvironment.get_execution_environment(config)

    # Add the kafka producer and consumers
    topic = "input-topic"
    broker = "localhost:9092"
    ds = add_kafka_source(env, topic)
    producer = Producer({"bootstrap.servers": 'localhost:9092'})
    deserialization_schema = ByteSerializer()
    properties: dict = {
        "bootstrap.servers": broker,
        "group.id": "test_group_1",
    }
    kafka_external_sink = FlinkKafkaProducer("out-topic", deserialization_schema, properties)
    kafka_internal_sink = FlinkKafkaProducer(topic, deserialization_schema, properties)

    # Create the datastream that will handle 
    # - simple (single node) dataflows and,
    # - init classes
    stream = (
        ds.map(lambda x: pickle.loads(x))
    )


    select_all_op = SelectAllOperator({Hotel: [hotel.name for hotel in hotels]})

    select_all_stream = (
        stream.filter(lambda e: isinstance(e.target, SelectAllNode))
            .process(select_all_op) # yield all the hotel_ids
    )

    op_stream = (
        stream.union(select_all_stream).filter(lambda e: isinstance(e.target, OpNode))
    )


    hotel_stream = (
        op_stream
            .filter(lambda e: e.target.cls == Hotel)
            .key_by(lambda e: e.key_stack[-1])
            .process(hotel_op)
    )

    full_stream = hotel_stream #.union...

    full_stream_filtered = (
        full_stream
            .filter(lambda e: isinstance(e, Event))
            .filter(lambda e: isinstance(e.target, Filter))
            .filter(lambda e: e.target.filter_fn())    
    )

    full_stream_unfiltered = (
        full_stream
            .filter(lambda e: not isinstance(e, Event) or not isinstance(e.target, Filter))
    )

    # have to remove items from full_stream as well??
    ds = full_stream_unfiltered.union(full_stream_filtered)

    # INIT HOTELS
    init_hotel = OpNode(Hotel, InitClass())
    for hotel in hotels:
        event = Event(init_hotel, [hotel.name], {"name": hotel.name, "loc": hotel.loc}, None) 
        producer.produce(
            topic,
            value=pickle.dumps(event),
        )

    

    ds_external = ds.map(lambda e: dbg(e)).filter(lambda e: isinstance(e, EventResult)).filter(lambda e: e.event_id > 99).print() #.add_sink(kafka_external_sink)
    ds_internal = ds.map(lambda e: dbg(e)).filter(lambda e: isinstance(e, Event)).map(lambda e: pickle.dumps(e)).add_sink(kafka_internal_sink)
    producer.flush()

    env.execute_async()

    print("sleepin")
    time.sleep(2)

    # GET NEARBY
    # dataflow for getting all hotels within region
    df = DataFlow("get_nearby_hotels")
    n0 = SelectAllNode(Hotel)
    n1 = OpNode(Hotel, Filter(get_nearby_predicate_compiled))
    df.add_edge(Edge(n0, n1))

    dist = 5
    loc = Geo(0, 0)
    event = Event(n0, [], {"loc": loc, "dist": dist}, df) 
    producer.produce(
        topic,
        value=pickle.dumps(event),
    )

    nearby = []
    for hotel in hotels:
        if hotel.distance(loc) < dist:
            nearby.append(hotel.name)
    print(nearby)
    # ok thats pretty good. But now we need to solve the problem of merging
    # an arbitray number of nodes. but like we naturally want to merge as late
    # as possible, right? ideally we want to process results in a streaming
    # fashion

    # I want another example that does something after filtering,
    # for example buying all items less than 10 price
    input()

