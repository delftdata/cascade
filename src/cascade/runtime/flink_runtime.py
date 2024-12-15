import os
from typing import Union
from pyflink.common.typeinfo import Types, get_gateway
from pyflink.common import Configuration, DeserializationSchema, SerializationSchema
from pyflink.datastream.data_stream import CloseableIterator
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, ValueState, ValueStateDescriptor
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream import StreamExecutionEnvironment
import pickle 
from cascade.dataflow.dataflow import Event, EventResult, InitClass, InvokeMethod, MergeNode, OpNode
from cascade.dataflow.operator import StatefulOperator
from confluent_kafka import Producer
import logging

logger = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

class FlinkOperator(KeyedProcessFunction):
    """Wraps an `cascade.dataflow.datflow.StatefulOperator` in a KeyedProcessFunction so that it can run in Flink.
    """
    def __init__(self, operator: StatefulOperator) -> None:
        self.state: ValueState = None # type: ignore (expect state to be initialised on .open())
        self.operator = operator

    
    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("state", Types.PICKLED_BYTE_ARRAY())
        self.state: ValueState = runtime_context.get_state(descriptor)

    def process_element(self, event: Event, ctx: KeyedProcessFunction.Context):
        key_stack = event.key_stack
        assert(isinstance(event.target, OpNode)) # should be handled by filters on this FlinkOperator
        logger.debug(f"FlinkOperator {event.target.cls.__name__}[{ctx.get_current_key()}]: Processing: {event}")
        if isinstance(event.target.method_type, InitClass):
            # TODO: compile __init__ with only kwargs, and pass the variable_map itself
            # otherwise, order of variable_map matters for variable assignment
            result = self.operator.handle_init_class(*event.variable_map.values())
            # Pop this key from the key stack so that we exit
            key_stack.pop()
            self.state.update(pickle.dumps(result))
        elif isinstance(event.target.method_type, InvokeMethod):
            state = pickle.loads(self.state.value())
            result = self.operator.handle_invoke_method(event.target.method_type, variable_map=event.variable_map, state=state, key_stack=key_stack)
            
            # TODO: check if state actually needs to be updated
            if state is not None:
                self.state.update(pickle.dumps(state))

        new_events = event.propogate(key_stack, result)
        if isinstance(new_events, EventResult):
            logger.debug(f"FlinkOperator {event.target.cls.__name__}[{ctx.get_current_key()}]: Returned {new_events}")
            yield new_events
        else:
            logger.debug(f"FlinkOperator {event.target.cls.__name__}[{ctx.get_current_key()}]: Propogated {len(new_events)} new Events")
            yield from new_events


class FlinkMergeOperator(KeyedProcessFunction):
    """Flink implementation of a merge operator."""
    def __init__(self) -> None:
        self.other: ValueState = None # type: ignore (expect state to be initialised on .open())
    
    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("merge_state", Types.PICKLED_BYTE_ARRAY())
        self.other = runtime_context.get_state(descriptor)

    def process_element(self, event: Event, ctx: KeyedProcessFunction.Context):
        other_args = self.other.value()
        logger.debug(f"FlinkMergeOp [{ctx.get_current_key()}]: Processing: {event}")
        if other_args == None:
            logger.debug(f"FlinkMergeOp [{ctx.get_current_key()}]: Saving merge value: {event.args}")
            self.other.update(event.args)
        else:
            self.other.clear() 
            merged_args = [*event.args, *other_args]
            logger.debug(f"FlinkMergeOp [{ctx.get_current_key()}]: Yielding merge values: {merged_args}")
            new_event = event.propogate(event.key_stack, [*event.args, *other_args], {})
            yield from new_event


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

INPUT_TOPIC = "input-topic"
"""@private"""

class FlinkRuntime():
    """A Runtime that runs Dataflows on Flink."""
    def __init__(self):
        self.env: StreamExecutionEnvironment = None
        """@private"""

        self.producer: Producer = None
        """@private"""

        self.sent_events = 0
        """The number of events that were sent using `send()`."""

    def init(self, kafka_broker="localhost:9092", bundle_time=1, bundle_size=5):
        """Initialise & configure the Flink runtime. 
        
        This function is required before any other calls, and requires a Kafka 
        broker to be ready at the specified address.

        :param kafka_broker: The URL of the Kafka service.

        :param bundle_time:  The waiting timeout (in ms) before processing a 
            bundle for Python UDF execution. This defines how long the elements
            of a bundle will be buffered before being processed. Lower timeouts
            lead to lower tail latencies but may affect throughput.

        :param bundle_size: The maximum number of elements to include in a
            bundle for Python UDF execution. The elements are processed
            asynchronously. One bundle of elements are processed before
            processing the next bundle of elements. A larger value can improve
            throughput but at the cost of more memory usage and higher latency.
        """
        config = Configuration()
        # Add the Flink Web UI at http://localhost:8081
        # config.set_string("rest.port", "8081")

        # Sets the waiting timeout(in milliseconds) before processing a bundle for Python user-defined function execution. 
        # The timeout defines how long the elements of a bundle will be buffered before being processed.
        # Lower timeouts lead to lower tail latencies, but may affect throughput.
        config.set_integer("python.fn-execution.bundle.time", bundle_time)

        # The maximum number of elements to include in a bundle for Python user-defined function execution. 
        # The elements are processed asynchronously. One bundle of elements are processed before processing the next bundle of elements. 
        # A larger value can improve the throughput, but at the cost of more memory usage and higher latency.
        config.set_integer("python.fn-execution.bundle.size", bundle_size)

        self.env = StreamExecutionEnvironment.get_execution_environment(config)

        kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
        'bin/flink-sql-connector-kafka-3.3.0-1.20.jar')
        serializer_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'bin/flink-kafka-bytes-serializer.jar')

        if os.name == 'nt':
            self.env.add_jars(f"file:///{kafka_jar}",f"file:///{serializer_jar}")
        else:
            self.env.add_jars(f"file://{kafka_jar}",f"file://{serializer_jar}")

        deserialization_schema = ByteSerializer()
        properties: dict = {
            "bootstrap.servers": kafka_broker,
            "auto.offset.reset": "earliest", 
            "group.id": "test_group_1",
        }
        kafka_consumer = FlinkKafkaConsumer(INPUT_TOPIC, deserialization_schema, properties)
        self.kafka_internal_sink = FlinkKafkaProducer(INPUT_TOPIC, deserialization_schema, properties)
        """Kafka sink that will be ingested again by the Flink runtime."""

        stream = (
            self.env.add_source(kafka_consumer)
                .map(lambda x: pickle.loads(x))
                # .filter(lambda e: isinstance(e, Event)) # Enforced by `add_operator` type safety
            )
        
        self.stateful_op_stream = stream.filter(lambda e: isinstance(e.target, OpNode))
        """Stream that ingests events with an `cascade.dataflow.dataflow.OpNode` target"""
        
        self.merge_op_stream = (
            stream.filter(lambda e: isinstance(e.target, MergeNode))
                .key_by(lambda e: e._id) # might not work in the future if we have multiple merges in one dataflow?
                .process(FlinkMergeOperator())
        )
        """Stream that ingests events with an `cascade.dataflow.dataflow.MergeNode` target"""

        self.stateful_op_streams = []
        """List of stateful operator streams, which gets appended at `add_operator`."""

        self.producer = Producer({'bootstrap.servers': kafka_broker})
    
    def add_operator(self, op: FlinkOperator):
        """Add a `FlinkOperator` to the Flink datastream."""
        op_stream = (
            self.stateful_op_stream.filter(lambda e: e.target.cls == op.operator._cls)
                .key_by(lambda e: e.key_stack[-1])
                .process(op)
            )
        self.stateful_op_streams.append(op_stream)

    def send(self, event: Event, flush=False):
        """Send an event to the Kafka source.
        Once `run` has been called, the Flink runtime will start ingesting these 
        messages. Messages can always be sent after `init` is called - Flink 
        will continue ingesting messages after `run` is called asynchronously.
        """
        self.producer.produce(INPUT_TOPIC, value=pickle.dumps(event))
        if flush:
            self.producer.flush()
        self.sent_events += 1

    def run(self, run_async=False, collect=False) -> Union[CloseableIterator, None]:
        """Start ingesting and processing messages from the Kafka source.
        
        If `collect` is True then this will return a CloseableIterator over 
        `cascade.dataflow.dataflow.EventResult`s."""

        # Combine all the operator streams
        ds = self.merge_op_stream.union(*self.stateful_op_streams)
        if collect:
            ds_external = ds.filter(lambda e: isinstance(e, EventResult)).execute_and_collect() 
        else:
            ds_external = ds.filter(lambda e: isinstance(e, EventResult)).print() #.add_sink(kafka_external_sink) 
        ds_internal = ds.filter(lambda e: isinstance(e, Event)).add_sink(self.kafka_internal_sink)

        if run_async:
            self.env.execute_async("Cascade: Flink Runtime")
            return ds_external # type: ignore (will be CloseableIterator provided the source is unbounded (i.e. Kafka))
        else:
            self.env.execute("Cascade: Flink Runtime")