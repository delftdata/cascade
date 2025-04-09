import logging
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import cascade
from cascade.runtime.flink_runtime import FlinkClientSync
from cascade.dataflow.dataflow import DataflowRef
from tests.integration.flink.utils import create_topics, init_flink_runtime, wait_for_event_id
from pyflink.datastream.data_stream import CloseableIterator


KAFKA_BROKER = "localhost:9092"
KAFKA_FLINK_BROKER = "kafka:9093" # If running a flink cluster and kafka inside docker, the broker url might be different

IN_TOPIC = "ds-movie-in"
OUT_TOPIC = "ds-movie-out"
INTERNAL_TOPIC = "ds-movie-internal"



def main():
    create_topics()

    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")
    runtime = init_flink_runtime("experiments.dynamic_prefetching.entities", parallelism=4)

    print(cascade.core.dataflows.keys())
    client = FlinkClientSync()
    
    runtime.run(run_async=True)
    # assert isinstance(collector, CloseableIterator)


    try:
        run_test(client)
    finally:
        client.close()

import time
def run_test(client):
    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")
    baseline = cascade.core.dataflows[DataflowRef("Prefetcher", "baseline")]
    prefetch = cascade.core.dataflows[DataflowRef("Prefetcher", "prefetch")]

    for block in baseline.blocks.values():
        print(block.function_string)

    for block in prefetch.blocks.values():
        print(block.function_string)

    event = baseline.generate_event({"branch_chance_0": 0.0})
    print(event)
    result = client.send(event, block=True)
    print(result)

    # for _ in range(10):
    #     event = baseline.generate_event({"branch_chance_0": 0.5})
    #     client.send(event)
    #     result = wait_for_event_id(event[0]._id, collector)
    #     print(result.result)



if __name__ == "__main__":
    main()