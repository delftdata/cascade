import cascade
from cascade.dataflow.dataflow import DataflowRef
from cascade.dataflow.optimization.parallelization import parallelize, parallelize_until_if
from tests.integration.flink.utils import create_topics, init_flink_runtime


KAFKA_BROKER = "localhost:9092"
KAFKA_FLINK_BROKER = "kafka:9093" # If running a flink cluster and kafka inside docker, the broker url might be different

IN_TOPIC = "prefetcher-in"
OUT_TOPIC = "prefetcher-out"
INTERNAL_TOPIC = "prefetcher-internal"



def main():
    create_topics(IN_TOPIC, OUT_TOPIC, INTERNAL_TOPIC)

    runtime = init_flink_runtime("experiments.dynamic_prefetching.entities", IN_TOPIC, OUT_TOPIC, INTERNAL_TOPIC, kafka_broker=KAFKA_FLINK_BROKER,bundle_time=5, bundle_size=10, thread_mode=True, parallelism=None)


    print(cascade.core.dataflows.keys())


    baseline = cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_baseline")]
    prefetch = cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_prefetch")]


    pre_par = parallelize(prefetch)
    cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_prefetch_parallel")] = pre_par
    runtime.add_dataflow(pre_par)

    base_par = parallelize(baseline)
    cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_baseline_parallel")] = base_par
    runtime.add_dataflow(base_par)

    runtime.run()

if __name__ == "__main__":
    main()