from typing import Literal
import cascade
from cascade.dataflow.dataflow import DataflowRef
from cascade.dataflow.optimization.dead_node_elim import dead_node_elimination
from cascade.dataflow.optimization.parallelization import parallelize_until_if
from cascade.runtime.flink_runtime import FlinkRuntime
from tests.integration.flink.utils import create_topics, init_flink_runtime

import os

KAFKA_BROKER = "localhost:9092"
KAFKA_FLINK_BROKER = "kafka:9093" # If running a flink cluster and kafka inside docker, the broker url might be different

IN_TOPIC = "ds-movie-in"
OUT_TOPIC = "ds-movie-out"
INTERNAL_TOPIC = "ds-movie-internal"

EXPERIMENT: Literal["baseline", "parallel"] = os.getenv("EXPERIMENT", "baseline")


def main():
    create_topics(IN_TOPIC, OUT_TOPIC, INTERNAL_TOPIC)

    runtime = init_flink_runtime("deathstar_movie_review.entities.entities", IN_TOPIC, OUT_TOPIC, INTERNAL_TOPIC, kafka_broker=KAFKA_FLINK_BROKER,bundle_time=5, bundle_size=10, thread_mode=True, parallelism=None)
       
    print(f"Creating dataflow [{EXPERIMENT}]")

    # for parallel experiment
    df_baseline = cascade.core.dataflows[DataflowRef("Frontend", "compose")]
    df_parallel, _ = parallelize_until_if(df_baseline)
    df_parallel.name = "compose_parallel"
    cascade.core.dataflows[DataflowRef("Frontend", "compose_parallel")] = df_parallel
    runtime.add_dataflow(df_parallel)

    # for prefetch experiment
    df_baseline = cascade.core.dataflows[DataflowRef("MovieId", "upload_movie_prefetch")]
    df_parallel, _ = parallelize_until_if(df_baseline)
    df_parallel.name = "upload_movie_prefetch_parallel"
    cascade.core.dataflows[DataflowRef("MovieId", "upload_movie_prefetch_parallel")] = df_parallel
    runtime.add_dataflow(df_parallel)

    print(cascade.core.dataflows.keys())
    
    runtime.run()

if __name__ == "__main__":
    main()

