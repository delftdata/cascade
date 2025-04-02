"""A test script for dataflows with merge operators"""

from pyflink.datastream.data_stream import CloseableIterator
import tests.integration.flink.utils as utils
from tests.integration.flink.utils import wait_for_event_id

import pytest

import cascade
import logging

@pytest.mark.integration
def test_stateful_operator():
    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")
    
    utils.create_topics()

    runtime, client = utils.init_flink_runtime("tests.integration.common")
    collector = runtime.run(run_async=True, output="collect")
    assert isinstance(collector, CloseableIterator)

    try:
        _test_stateful_operator(client, collector)
    finally:
        collector.close()
        client.close()


def _test_stateful_operator(client, collector):
    
    user_op = cascade.core.operators["User"]
    item_op = cascade.core.operators["Item"]
    event = user_op.dataflows["__init__"].generate_event({"key": "foo", "balance": 100}, key="foo")
    client.send(event)

    result = wait_for_event_id(event[0]._id, collector)
    print(result.result.__dict__)

    event = item_op.dataflows["__init__"].generate_event({"key": "fork", "price": 5}, key="fork")
    client.send(event)

    event = item_op.dataflows["__init__"].generate_event({"key": "spoon", "price": 3}, key="spoon")
    client.send(event)

    result = wait_for_event_id(event[0]._id, collector)
    print(result.result.__dict__)


    print(user_op.dataflows["buy_2_items"].to_dot())
      
    # Buy a fork and spoon
    event = user_op.dataflows["buy_2_items"].generate_event({"item1_0": "fork", "item2_0": "spoon"}, key="foo")
    client.send(event)
    result = wait_for_event_id(event[0]._id, collector)
    assert result.result == True
   

    # Check the balance
    event = user_op.dataflows["get_balance"].generate_event({}, key="foo")
    client.send(event)
    result = wait_for_event_id(event[0]._id, collector)
    assert result.result == (100 - 5 - 3)
