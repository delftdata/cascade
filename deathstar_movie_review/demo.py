from typing import Literal
from cascade.dataflow.optimization.dead_node_elim import dead_node_elimination
from cascade.runtime.flink_runtime import FlinkRuntime

from .entities.user import user_op
from .entities.compose_review import compose_review_op
from .entities.frontend import frontend_df_parallel, frontend_df_serial, frontend_op, text_op, unique_id_op
from .entities.movie import movie_id_op, movie_info_op, plot_op

import os
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA_BROKER = "localhost:9092"
KAFKA_FLINK_BROKER = "kafka:9093" # If running a flink cluster and kafka inside docker, the broker url might be different

IN_TOPIC = "ds-movie-in"
OUT_TOPIC = "ds-movie-out"
INTERNAL_TOPIC = "ds-movie-internal"

EXPERIMENT: Literal["baseline", "pipelined", "parallel"] = os.getenv("EXPERIMENT", "baseline")

def create_topics(*required_topics):
    conf = {
        "bootstrap.servers": KAFKA_BROKER
    }

    admin_client = AdminClient(conf)

    # Fetch existing topics
    existing_topics = admin_client.list_topics(timeout=5).topics.keys()

    # Find missing topics
    missing_topics = [topic for topic in required_topics if topic not in existing_topics]

    if missing_topics:
        print(f"Creating missing topics: {missing_topics}")
        
        # Define new topics (default: 1 partition, replication factor 1)
        new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in missing_topics]

        # Create topics
        futures = admin_client.create_topics(new_topics)

        # Wait for topic creation to complete
        for topic, future in futures.items():
            try:
                future.result()  # Block until the operation is complete
                print(f"Topic '{topic}' created successfully")
            except Exception as e:
                print(f"Failed to create topic '{topic}': {e}")
    else:
        print("All required topics exist.")


def main():
    create_topics(IN_TOPIC, OUT_TOPIC, INTERNAL_TOPIC)

    runtime = FlinkRuntime(IN_TOPIC, OUT_TOPIC, internal_topic=INTERNAL_TOPIC)
    runtime.init(kafka_broker=KAFKA_FLINK_BROKER,bundle_time=5, bundle_size=10)

    if EXPERIMENT == "baseline":
        frontend_op.dataflow = frontend_df_serial()
    elif EXPERIMENT == "pipelined":
        frontend_op.dataflow = frontend_df_serial()
        dead_node_elimination([], [frontend_op])
    elif EXPERIMENT == "parallel":
        frontend_op.dataflow = frontend_df_parallel()

    print(frontend_op.dataflow.to_dot())
    print(f"Creating dataflow [{EXPERIMENT}]")

    runtime.add_operator(compose_review_op)
    runtime.add_operator(user_op)
    runtime.add_operator(movie_info_op)
    runtime.add_operator(movie_id_op)
    runtime.add_operator(plot_op)
    runtime.add_stateless_operator(frontend_op)
    runtime.add_stateless_operator(unique_id_op)
    runtime.add_stateless_operator(text_op)

    runtime.run()

if __name__ == "__main__":
    main()

