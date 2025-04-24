import cascade
from cascade.dataflow.dataflow import DataflowRef
from cascade.dataflow.optimization.parallelization import parallelize_until_if
from experiments.dynamic_prefetching.run_prefetcher import gen_parallel
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

    baseline = cascade.core.dataflows[DataflowRef("Prefetcher", "baseline")]
    prefetch = cascade.core.dataflows[DataflowRef("Prefetcher", "prefetch")]

    pre_par = gen_parallel(prefetch)
    cascade.core.dataflows[DataflowRef("Prefetcher", "prefetch_parallel")] = pre_par
    runtime.add_dataflow(pre_par)

    base_par = gen_parallel(baseline)
    cascade.core.dataflows[DataflowRef("Prefetcher", "baseline_parallel")] = base_par
    runtime.add_dataflow(base_par)

    runtime.run()

if __name__ == "__main__":
    main()