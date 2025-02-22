from cascade.runtime.flink_runtime import FlinkRuntime

from .entities.user import user_op
from .entities.compose_review import compose_review_op
from .entities.frontend import frontend_op, text_op, unique_id_op
from .entities.movie import movie_id_op, movie_info_op, plot_op


from confluent_kafka.admin import AdminClient, NewTopic


def create_topics():
    # Kafka broker configuration
    conf = {
        "bootstrap.servers": "localhost:9092"  # Replace with your Kafka broker(s)
    }

    # Create an AdminClient
    admin_client = AdminClient(conf)

    # Topics to ensure
    required_topics = ["ds-movie-in", "internal-topic", "ds-movie-out"]

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
    create_topics()
    runtime = FlinkRuntime("ds-movie-in", "ds-movie-out")
    runtime.init(kafka_broker="kafka:9093",bundle_time=5, bundle_size=10)

    # print(frontend_op.dataflow.to_dot())
    # dead_node_elimination([], [frontend_op])
    print(frontend_op.dataflow.to_dot())
    # input()


    runtime.add_operator(compose_review_op)
    runtime.add_operator(user_op)
    runtime.add_operator(movie_info_op)
    runtime.add_operator(movie_id_op)
    runtime.add_operator(plot_op)
    runtime.add_stateless_operator(frontend_op)
    runtime.add_stateless_operator(unique_id_op)
    runtime.add_stateless_operator(text_op)

    print("running now...")
    runtime.run()
    print("running")
    # input("wait!")
    # print()
    # populate_user(runtime)
    # populate_movie(runtime)
    # runtime.producer.flush()
    # time.sleep(1)

    # input()

    # # with Pool(threads) as p:
    # #     results = p.map(benchmark_runner, range(threads))

    # # results = {k: v for d in results for k, v in d.items()}
    # results = benchmark_runner(0)

    # print("last result:")
    # print(list(results.values())[-1])
    # t = len(results)
    # r = 0
    # for result in results.values():
    #     if result["ret"] is not None:
    #         print(result)
    #         r += 1
    # print(f"{r}/{t} results recieved.")
    # write_dict_to_pkl(results, "test2.pkl")

if __name__ == "__main__":
    main()

