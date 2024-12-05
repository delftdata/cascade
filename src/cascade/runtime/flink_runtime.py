import os
from pyflink.common.typeinfo import Types, get_gateway
from pyflink.common import Configuration, DeserializationSchema, SerializationSchema
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, ValueState
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream import StreamExecutionEnvironment
import pickle 
from confluent_kafka import Producer

class ByteSerializer(SerializationSchema, DeserializationSchema):
    """A custom serializer which maps bytes to bytes.

    The implementation consist of a jar file added through `env.add_jars` with the following code:
    
    ```java
    public class KafkaBytesSerializer implements SerializationSchema<byte[]>,
        DeserializationSchema<byte[]> {

            @Override
            public byte[] deserialize(byte[] bytes) throws IOException {
                return bytes;
            }

            @Override
            public boolean isEndOfStream(byte[] bytes) {
                return false;
            }

            @Override
            public byte[] serialize(byte[] bytes) {
                return bytes;
            }

            @Override
            public TypeInformation<byte[]> getProducedType() {
                return TypeInformation.of(byte[].class);
            }
        }
    ```

    See: https://lists.apache.org/thread/7bnml93oohy3p9bv3y4jltktrdgs8kcg
    """
    def __init__(self):
        gate_way = get_gateway()

        j_byte_string_schema = gate_way.jvm.nl.tudelft.stateflow.KafkaBytesSerializer() # type: ignore

        SerializationSchema.__init__(self, j_serialization_schema=j_byte_string_schema)
        DeserializationSchema.__init__(
            self, j_deserialization_schema=j_byte_string_schema
        )

class FlinkOperator(KeyedProcessFunction):
    pass

INPUT_TOPIC = "input-topic"
"""@private"""

class FlinkRuntime():
    def __init__(self):
        self.env: StreamExecutionEnvironment = None
        self.producer: Producer = None

    def _initialise(self, kafka_broker="localhost:9092"):
        config = Configuration()
        # Add the Flink Web UI at http://localhost:8081
        config.set_string("rest.port", "8081")
        self.env = StreamExecutionEnvironment.get_execution_environment()

        kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
        'bin/flink-sql-connector-kafka-3.3.0-1.20.jar')
        serializer_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'bin/flink-kafka-bytes-serializer.jar')

        if os.name == 'nt':
            self.env.add_jars(f"file:///{kafka_jar}",f"file://{serializer_jar}")
        else:
            self.env.add_jars(f"file://{kafka_jar}",f"file://{serializer_jar}")

        deserialization_schema = ByteSerializer()
        properties: dict = {
            "bootstrap.servers": kafka_broker,
            "auto.offset.reset": "earliest", 
            "group.id": "test_group_1",
        }
        kafka_consumer = FlinkKafkaConsumer(INPUT_TOPIC, deserialization_schema, properties)
        
        # Pass through
        self.env.add_source(kafka_consumer).map(lambda x: pickle.loads(x)).print()

        self.producer = Producer({'bootstrap.server': kafka_broker})

    def _send(self, event):
        self.producer.produce(INPUT_TOPIC, value=pickle.dumps(event))

    def run(self):
        self.env.execute("Cascade: Flink Runtime")