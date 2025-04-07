"""A test script for dataflows with merge operators"""

from pyflink.datastream.data_stream import CloseableIterator
from cascade.dataflow.dataflow import DataflowRef
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

    user_buy_2 = cascade.core.dataflows[DataflowRef("User", "buy_2_items")]
    item_init = cascade.core.dataflows[DataflowRef("Item", "__init__")]
    user_init = cascade.core.dataflows[DataflowRef("User", "__init__")]
    user_get_balance = cascade.core.dataflows[DataflowRef("User", "get_balance")]

    df_parallel = parallelize(user_buy_2)
    df_parallel.name = "buy_2_parallel"
    cascade.core.dataflows[DataflowRef("User", "buy_2_parallel")] = df_parallel
    print(df_parallel.to_dot())
    assert len(df_parallel.entry) == 2


    event = user_init.generate_event({"key": "foo", "balance": 100}, key="foo")
    client.send(event)

    result = wait_for_event_id(event[0]._id, collector)

    event = item_init.generate_event({"key": "fork", "price": 5}, key="fork")
    client.send(event)

    event = item_init.generate_event({"key": "spoon", "price": 3}, key="spoon")
    client.send(event)

    result = wait_for_event_id(event[0]._id, collector)

      
    # Buy a fork and spoon
    print("sending buy 2")
    print(df_parallel.to_dot())
    event = df_parallel.generate_event({"item1_0": "fork", "item2_0": "spoon"}, key="foo")
    print(event)
    client.send(event)
    result = wait_for_event_id(event[0]._id, collector)
    assert result.result == True
   

    # Check the balance
    event = user_get_balance.generate_event({}, key="foo")
    client.send(event)
    result = wait_for_event_id(event[0]._id, collector)
    assert result.result == (100 - 5 - 3)


