import cascade
from cascade.dataflow.dataflow import DataflowRef
from cascade.dataflow.optimization.parallelization import parallelize_until_if
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

    print(baseline.to_dot())

    par, rest = parallelize_until_if(prefetch)

    # join the two dataflows
    par_exit = [node.id for node in par.nodes.values() if len(node.outgoing_edges) == 0]
    for edge in rest.edges:
        par.add_edge(edge)
    assert len(rest.entry) == 1
    assert len(par_exit) == 1
    par.add_edge_refs(par_exit[0], rest.entry[0].id, None)


    print(par.to_dot())
    par.name = "prefetch_parallel"
    cascade.core.dataflows[DataflowRef("Prefetcher", "prefetch_parallel")] = par

    runtime.add_dataflow(par)
    
    runtime.run()

if __name__ == "__main__":
    main()