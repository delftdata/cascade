"""A test script for dataflows with merge operators"""

from pyflink.datastream.data_stream import CloseableIterator
from cascade.dataflow.optimization.parallelization import parallelize

import tests.integration.flink.utils as utils
from tests.integration.flink.utils import wait_for_event_id
import pytest

import cascade
import logging

@pytest.mark.integration
def test_collect_operator():
    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")
    
    utils.create_topics()

    runtime, client = utils.init_flink_runtime("tests.integration.common")
    collector = runtime.run(run_async=True, output="collect")
    assert isinstance(collector, CloseableIterator)

    try:
        _test_collect_operator(client, collector)
    finally:
        collector.close()
        client.close()


def _test_collect_operator(client, collector):
    user_op = cascade.core.operators["User"]
    item_op = cascade.core.operators["Item"]
    df = parallelize(user_op.dataflows["buy_2_items"])
    df.name = "buy_2_parallel"
    user_op.dataflows["buy_2_parallel"] = df
    print(user_op.dataflows["buy_2_parallel"].to_dot())
    print(user_op.dataflows)
    assert len(user_op.dataflows["buy_2_parallel"].entry) == 2


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

      
    # Buy a fork and spoon
    event = user_op.dataflows["buy_2_parallel"].generate_event({"item1_0": "fork", "item2_0": "spoon"}, key="foo")
    client.send(event)
    result = wait_for_event_id(event[0]._id, collector)
    assert result.result == True
   

    # Check the balance
    event = user_op.dataflows["get_balance"].generate_event({}, key="foo")
    client.send(event)
    result = wait_for_event_id(event[0]._id, collector)
    assert result.result == (100 - 5 - 3)
