import cascade
from cascade.dataflow.dataflow import EventResult
from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from cascade.runtime.flink_runtime import FlinkClientSync, FlinkRuntime
from confluent_kafka.admin import AdminClient, NewTopic
from pyflink.datastream.data_stream import CloseableIterator


KAFKA_BROKER = "localhost:9092"

IN_TOPIC = "input-topic"
OUT_TOPIC = "output-topic"
INTERNAL_TOPIC = "internal-topic"

def wait_for_event_id(id: int, collector: CloseableIterator) -> EventResult:
    for record in collector:
        print(f"Collected record: {record}")
        if record.event_id == id:
            return record
        

def init_cascade_from_module(import_path: str):
    cascade.core.clear()
    exec(f'import {import_path}')
    cascade.core.init()

def init_flink_runtime(import_path: str, in_topic=None, out_topic=None, internal_topic=None, parallelism=None, **init_args) -> FlinkRuntime:
    init_cascade_from_module(import_path)

    if in_topic is None:
        in_topic = IN_TOPIC
    if out_topic is None:
        out_topic = OUT_TOPIC
    if internal_topic is None:
        internal_topic = INTERNAL_TOPIC

    runtime = FlinkRuntime(in_topic, out_topic, internal_topic=internal_topic)
    
    for op in cascade.core.operators.values():
        if isinstance(op, StatefulOperator):
            runtime.add_operator(op)
        elif isinstance(op, StatelessOperator):
            runtime.add_stateless_operator(op)
    
    runtime.init(parallelism=parallelism, **init_args)
    return runtime

def create_topics(*required_topics):
    if len(required_topics) == 0:
        required_topics = (IN_TOPIC, OUT_TOPIC, INTERNAL_TOPIC)
        
    conf = {
        "bootstrap.servers": KAFKA_BROKER
    }

    admin_client = AdminClient(conf)

    # Define new topics (default: 1 partition, replication factor 1)
    new_topics = [NewTopic(topic, num_partitions=32, replication_factor=1) for topic in required_topics]

    # Delete topics
    futures = admin_client.delete_topics(list(required_topics))
    for topic, future in futures.items():
        try:
            future.result()  # Block until the operation is complete
            print(f"Topic '{topic}' deleted successfully")
        except Exception as e:
            print(f"Failed to delete topic '{topic}': {e}")

    # Create topics
    futures = admin_client.create_topics(new_topics)
    for topic, future in futures.items():
        try:
            future.result()  # Block until the operation is complete
            print(f"Topic '{topic}' recreated successfully")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")