"""A test script for dataflows with multiple Operators
"""

import time

from pyflink.datastream.data_stream import CloseableIterator

from common import Item, User, item_op, user_op
from cascade.dataflow.dataflow import DataFlow, Edge, Event, EventResult, InitClass, InvokeMethod, Node, OpNode
from cascade.runtime.flink_runtime import FlinkOperator, FlinkRuntime
import pytest

@pytest.mark.integration
def test_two_entities():
    runtime = FlinkRuntime()
    runtime.init()
    runtime.add_operator(FlinkOperator(item_op))
    runtime.add_operator(FlinkOperator(user_op))

    # 2. Utilisation

    # Create a User object
    foo_user = User("foo", 100)
    init_user_node = OpNode(User, InitClass())
    event = Event(init_user_node, ["foo"], {"key": "foo", "price": 100}, None) 

    runtime.send(event)

    # Create an Item object
    fork_item = Item("fork", 5)
    init_item_node = OpNode(Item, InitClass())
    event = Event(init_item_node, ["fork"], {"key": "fork", "price": 5}, None) 
    runtime.send(event)

    # Create an expensive Item
    house_item = Item("house", 1000)
    event = Event(init_item_node, ["house"], {"key": "house", "price": 1000}, None) 
    runtime.send(event)

    # Have the User object buy the item
    foo_user.buy_item(fork_item)
    # the dataflow created
    df = DataFlow("user.buy_item")
    n1 = OpNode(User, InvokeMethod("buy_item_0"))
    n2 = OpNode(Item, InvokeMethod("get_price"))
    n3 = OpNode(User, InvokeMethod("buy_item_1"))
    df.add_edge(Edge(n1, n2))
    df.add_edge(Edge(n2, n3))

    # User with key "foo" buys item with key "fork"
    user_buys_fork = Event(n1, ["foo"],  {"item_key": "fork"}, df)
    runtime.send(user_buys_fork, flush=True)

    collected_iterator: CloseableIterator = runtime.run(run_async=True, collect=True)
    records = []

    def wait_for_event_id(id: int) -> EventResult:
        for record in collected_iterator:
            records.append(record)
            print(f"Collected record: {record}")
            if record.event_id == id:
                return record

    # Check that we were able to buy the fork
    buy_fork_result = wait_for_event_id(user_buys_fork._id)
    assert buy_fork_result.result == True

    # Send an event to check if the balance was updated
    user_get_balance_node = OpNode(User, InvokeMethod("get_balance"))
    user_get_balance = Event(user_get_balance_node, ["foo"], {}, None) 
    runtime.send(user_get_balance, flush=True)

    # See that the user's balance has gone down
    get_balance = wait_for_event_id(user_get_balance._id)
    assert get_balance.result == 95

    # User with key "foo" buys item with key "house"
    user_buys_house = Event(n1, ["foo"],  {"item_key": "house"}, df)
    runtime.send(user_buys_house, flush=True) 

    # Balance becomes negative when house is bought
    buy_house_result = wait_for_event_id(user_buys_house._id)
    assert buy_house_result.result == False

    collected_iterator.close()

    print(records)