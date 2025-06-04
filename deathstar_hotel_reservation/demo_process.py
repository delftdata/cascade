import cascade
from tests.integration.flink.utils import create_topics, init_flink_runtime

KAFKA_BROKER = "localhost:9092"
KAFKA_FLINK_BROKER = "kafka:9093" # If running a flink cluster and kafka inside docker, the broker url might be different

IN_TOPIC = "ds-hotel-in"
OUT_TOPIC = "ds-hotel-out"
INTERNAL_TOPIC = "ds-hotel-internal"

def main():
    thread_mode = False
  
    create_topics(IN_TOPIC, OUT_TOPIC, INTERNAL_TOPIC)
    runtime = init_flink_runtime([
        "deathstar_hotel_reservation.entities.user",
        # "deathstar_hotel_reservation.entities.reservation"
    ], IN_TOPIC, OUT_TOPIC, INTERNAL_TOPIC, kafka_broker=KAFKA_FLINK_BROKER, bundle_time=5, bundle_size=10, thread_mode=thread_mode, parallelism=None)

    print(cascade.core.dataflows.keys())
    runtime.run()

if __name__ == "__main__":
    main()