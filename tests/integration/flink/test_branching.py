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
def test_branching_pyflink():
    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")
    
    utils.create_topics()

    runtime, client = utils.init_flink_runtime("tests.integration.branching")
    collector = runtime.run(run_async=True, output="collect")
    assert isinstance(collector, CloseableIterator)

    try:
        _test_branching(client, collector)
    finally:
        collector.close()
        client.close()


def _test_branching(client, collector):
    branch = cascade.core.dataflows[DataflowRef("Brancher", "branch")]
    print(branch.to_dot())

    event = branch.generate_event({"cond_0": True})
    client.send(event)
    result = wait_for_event_id(event[0]._id, collector)
    assert result.result == 33

    event = branch.generate_event({"cond_0": False})
    client.send(event)
    result = wait_for_event_id(event[0]._id, collector)
    assert result.result == 42