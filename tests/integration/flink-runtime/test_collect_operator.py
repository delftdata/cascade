"""A test script for dataflows with merge operators"""

from pyflink.datastream.data_stream import CloseableIterator
from common import Item, User, item_op, user_op
from cascade.dataflow.dataflow import Event, EventResult, InitClass, InvokeMethod, OpNode
from cascade.runtime.flink_runtime import FlinkOperator, FlinkRuntime
import pytest

@pytest.mark.integration
def test_merge_operator():
    runtime = FlinkRuntime("test_collect_operator")
    runtime.init()
    runtime.add_operator(item_op)
    runtime.add_operator(user_op)


    # Create a User object
    foo_user = User("foo", 100)
    init_user_node = OpNode(User, InitClass(), read_key_from="key")
    event = Event(init_user_node, {"key": "foo", "balance": 100}, None) 
    runtime.send(event)

    # Create an Item object
    fork_item = Item("fork", 5)
    init_item_node = OpNode(Item, InitClass(), read_key_from="key")
    event = Event(init_item_node, {"key": "fork", "price": 5}, None) 
    runtime.send(event)

    # Create another Item
    spoon_item = Item("spoon", 3)
    event = Event(init_item_node, {"key": "spoon", "price": 3}, None) 
    runtime.send(event, flush=True)

    collected_iterator: CloseableIterator = runtime.run(run_async=True, output="collect")
    records = []

    def wait_for_event_id(id: int) -> EventResult:
        for record in collected_iterator:
            records.append(record)
            print(f"Collected record: {record}")
            if record.event_id == id:
                return record

    # Make sure the user & items are initialised
    wait_for_event_id(event._id)

    # Have the User object buy the item
    foo_user.buy_2_items(fork_item, spoon_item)
    df = user_op.dataflows["buy_2_items"]

    # User with key "foo" buys item with key "fork"
    user_buys_cutlery = Event(df.entry, {"user_key": "foo", "item1_key": "fork", "item2_key": "spoon"}, df)
    runtime.send(user_buys_cutlery, flush=True)

    
    # Check that we were able to buy the fork
    buy_fork_result = wait_for_event_id(user_buys_cutlery._id)
    assert buy_fork_result.result == True

    # Send an event to check if the balance was updated
    user_get_balance_node = OpNode(User, InvokeMethod("get_balance"), read_key_from="key")
    user_get_balance = Event(user_get_balance_node, {"key": "foo"}, None) 
    runtime.send(user_get_balance, flush=True)

    # See that the user's balance has gone down
    get_balance = wait_for_event_id(user_get_balance._id)
    assert get_balance.result == 92

    collected_iterator.close()

    print(records)