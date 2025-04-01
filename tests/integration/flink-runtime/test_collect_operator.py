"""A test script for dataflows with merge operators"""

from pyflink.datastream.data_stream import CloseableIterator
from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from cascade.dataflow.dataflow import Event, EventResult, InitClass, InvokeMethod, OpNode
from cascade.runtime.flink_runtime import FlinkClientSync, FlinkOperator, FlinkRuntime
import pytest

import cascade

def init_flink_runtime() -> tuple[FlinkRuntime, FlinkClientSync]:
    cascade.core.clear()
    exec(f'import tests.integration.common')
    cascade.core.init()
    runtime = FlinkRuntime(IN_TOPIC, OUT_TOPIC, internal_topic=INTERNAL_TOPIC)
    
    for op in cascade.core.operators.values():
        if isinstance(op, StatefulOperator):
            runtime.add_operator(op)
        elif isinstance(op, StatelessOperator):
            runtime.add_stateless_operator(op)
    
    runtime.init(parallelism=4)
    return runtime, FlinkClientSync()

import os
from confluent_kafka.admin import AdminClient, NewTopic
import logging

KAFKA_BROKER = "localhost:9092"

IN_TOPIC = "input-topic"
OUT_TOPIC = "output-topic"
INTERNAL_TOPIC = "internal-topic"

def create_topics(*required_topics):
    conf = {
        "bootstrap.servers": KAFKA_BROKER
    }

    admin_client = AdminClient(conf)

    # Fetch existing topics
    existing_topics = admin_client.list_topics(timeout=5).topics.keys()

    # Find missing topics
    missing_topics = [topic for topic in required_topics if topic not in existing_topics]

    if missing_topics:
        print(f"Creating missing topics: {missing_topics}")
        
        # Define new topics (default: 1 partition, replication factor 1)
        new_topics = [NewTopic(topic, num_partitions=32, replication_factor=1) for topic in missing_topics]

        # Create topics
        futures = admin_client.create_topics(new_topics)

        # Wait for topic creation to complete
        for topic, future in futures.items():
            try:
                future.result()  # Block until the operation is complete
                print(f"Topic '{topic}' created successfully")
            except Exception as e:
                print(f"Failed to create topic '{topic}': {e}")
    else:
        print("All required topics exist.")

@pytest.mark.integration
def test_merge_operator():
    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")
    create_topics(IN_TOPIC, OUT_TOPIC, INTERNAL_TOPIC)
    runtime, client = init_flink_runtime()
    
    collected_iterator = runtime.run(run_async=True, output="collect")
    assert isinstance(collected_iterator, CloseableIterator)
    records = []

    def wait_for_event_id(id: int) -> EventResult:
        for record in collected_iterator:
            records.append(record)
            print(f"Collected record: {record}")
            if record.event_id == id:
                return record


    user_op = cascade.core.operators["User"]
    item_op = cascade.core.operators["Item"]
    event = user_op.dataflows["__init__"].generate_event({"key": "foo", "balance": 100}, key="foo")
    client.send(event)

    result = wait_for_event_id(event[0]._id)
    print(result.result.__dict__)

    event = item_op.dataflows["__init__"].generate_event({"key": "fork", "price": 5}, key="fork")
    client.send(event)

    event = item_op.dataflows["__init__"].generate_event({"key": "spoon", "price": 3}, key="spoon")
    client.send(event)

    result = wait_for_event_id(event[0]._id)
    print(result.result.__dict__)

   


    # # Have the User object buy the item
    # foo_user.buy_2_items(fork_item, spoon_item)
    # df = user_op.dataflows["buy_2_items"]

    event = user_op.dataflows["buy_2_items"].generate_event({"item1_0": "fork", "item2_0": "spoon"}, key="foo")
    client.send(event)
    result = wait_for_event_id(event[0]._id)
    assert result.result == True
   

    # Check the balance
    event = user_op.dataflows["get_balance"].generate_event({}, key="foo")
    client.send(event)
    result = wait_for_event_id(event[0]._id)
    assert result.result == 92

    collected_iterator.close()
    client.close()
    # # Send an event to check if the balance was updated
    # user_get_balance_node = OpNode(User, InvokeMethod("get_balance"), read_key_from="key")
    # user_get_balance = Event(user_get_balance_node, {"key": "foo"}, None) 
    # runtime.send(user_get_balance, flush=True)

    # # See that the user's balance has gone down
    # get_balance = wait_for_event_id(user_get_balance._id)
    # assert get_balance.result == 92

    # collected_iterator.close()

    # print(records)