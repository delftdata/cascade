"""A test script for dataflows with merge operators"""

from pyflink.datastream.data_stream import CloseableIterator
from cascade.dataflow.dataflow import DataflowRef, Event
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

    item_init = cascade.core.dataflows[DataflowRef("Item", "__init__")]
    user_init = cascade.core.dataflows[DataflowRef("User", "__init__")]
    user_buy_2 = cascade.core.dataflows[DataflowRef("User", "buy_2_items")]
    user_get_balance = cascade.core.dataflows[DataflowRef("User", "get_balance")]

    event = user_init.generate_event({"key": "foo", "balance": 100}, key="foo")
    client.send(event)

    result = wait_for_event_id(event[0]._id, collector)
    print(result.result.__dict__)

    event = item_init.generate_event({"key": "fork", "price": 5}, key="fork")
    client.send(event)

    event = item_init.generate_event({"key": "spoon", "price": 3}, key="spoon")
    client.send(event)

    result = wait_for_event_id(event[0]._id, collector)
    print(result.result.__dict__)


    print(user_buy_2.to_dot())
      
    # Buy a fork and spoon
    event = user_buy_2.generate_event({"item1_0": "fork", "item2_0": "spoon"}, key="foo")
    client.send(event)
    result = wait_for_event_id(event[0]._id, collector)
    assert result.result == True
   

    # Check the balance
    event = user_get_balance.generate_event({}, key="foo")
    client.send(event)
    result = wait_for_event_id(event[0]._id, collector)
    assert result.result == (100 - 5 - 3)


@pytest.mark.integration
def test_stateless_operator():
    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")
    
    utils.create_topics()

    runtime, client = utils.init_flink_runtime("tests.integration.stateless")
    collector = runtime.run(run_async=True, output="collect")
    assert isinstance(collector, CloseableIterator)

    try:
        _test_stateless_operator(client, collector)
    finally:
        collector.close()
        client.close()


def _test_stateless_operator(client, collector):
    user_op = cascade.core.operators["SomeStatelessOp"]
    event = cascade.core.dataflows[DataflowRef("SomeStatelessOp", "get")].generate_event({})
    client.send(event)

    result = wait_for_event_id(event[0]._id, collector)
    assert result.result == 42