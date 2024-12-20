from dataclasses import dataclass
import os
from typing import Optional, Type, Union
from pyflink.common.typeinfo import Types, get_gateway
from pyflink.common import Configuration, DeserializationSchema, SerializationSchema, WatermarkStrategy
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.data_stream import CloseableIterator
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, ValueState, ValueStateDescriptor
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSource, KafkaSink
from pyflink.datastream import ProcessFunction, StreamExecutionEnvironment
import pickle 
from cascade.dataflow.dataflow import Arrived, CollectNode, CollectTarget, Event, EventResult, Filter, InitClass, InvokeMethod, MergeNode, Node, NotArrived, OpNode, Operator, Result, SelectAllNode
from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from confluent_kafka import Producer
import logging

logger = logging.getLogger(__name__)
logger.setLevel(1)
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

@dataclass
class FlinkRegisterKeyNode(Node):
    """A node that will register a key with the SelectAll operator.
    
    This node is specific to Flink, and will be automatically generated.
    It should not be used in a `DataFlow`.
    
    @private
    """
    key: str
    cls: Type

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

        # should be handled by filters on this FlinkOperator    
        assert(isinstance(event.target, OpNode)) 
        assert(isinstance(event.target.operator, StatefulOperator)) 
        assert(event.target.operator.entity == self.operator.entity) 

        logger.debug(f"FlinkOperator {self.operator.entity.__name__}[{ctx.get_current_key()}]: Processing: {event.target.method_type}")
        if isinstance(event.target.method_type, InitClass):
            # TODO: compile __init__ with only kwargs, and pass the variable_map itself
            # otherwise, order of variable_map matters for variable assignment
            result = self.operator.handle_init_class(*event.variable_map.values())

            # Register the created key in FlinkSelectAllOperator
            register_key_event = Event(
                FlinkRegisterKeyNode(key_stack[-1], self.operator.entity),
                [],
                {},
                None,
                _id = event._id
            )
            logger.debug(f"FlinkOperator {self.operator.entity.__name__}[{ctx.get_current_key()}]: Registering key: {register_key_event}")
            yield register_key_event

            # Pop this key from the key stack so that we exit
            key_stack.pop()
            self.state.update(pickle.dumps(result))
        elif isinstance(event.target.method_type, InvokeMethod):
            state = pickle.loads(self.state.value())
            result = self.operator.handle_invoke_method(event.target.method_type, variable_map=event.variable_map, state=state, key_stack=key_stack)
            
            # TODO: check if state actually needs to be updated
            if state is not None:
                self.state.update(pickle.dumps(state))
        elif isinstance(event.target.method_type, Filter):
            state = pickle.loads(self.state.value())
            result = event.target.method_type.filter_fn(event.variable_map, state)
            if not result:
                return
            result = event.key_stack[-1] 
        
        if event.target.assign_result_to is not None:
            event.variable_map[event.target.assign_result_to] = result

        new_events = event.propogate(key_stack, result)
        if isinstance(new_events, EventResult):
            logger.debug(f"FlinkOperator {self.operator.entity.__name__}[{ctx.get_current_key()}]: Returned {new_events}")
            yield new_events
        else:
            logger.debug(f"FlinkOperator {self.operator.entity.__name__}[{ctx.get_current_key()}]: Propogated {len(new_events)} new Events")
            yield from new_events

class FlinkStatelessOperator(ProcessFunction):
    """Wraps an `cascade.dataflow.datflow.StatefulOperator` in a KeyedProcessFunction so that it can run in Flink.
    """
    def __init__(self, operator: StatelessOperator) -> None:
        self.state: ValueState = None # type: ignore (expect state to be initialised on .open())
        self.operator = operator


    def process_element(self, event: Event, ctx: KeyedProcessFunction.Context):
        key_stack = event.key_stack

        # should be handled by filters on this FlinkOperator    
        assert(isinstance(event.target, OpNode)) 
        assert(isinstance(event.target.operator, StatelessOperator))

        logger.debug(f"FlinkStatelessOperator {self.operator.dataflow.name}[{event._id}]: Processing: {event.target.method_type}")
        if isinstance(event.target.method_type, InvokeMethod):
            result = self.operator.handle_invoke_method(event.target.method_type, variable_map=event.variable_map, key_stack=key_stack)  
        else:
            raise Exception(f"A StatelessOperator cannot compute event type: {event.target.method_type}")
        
        if event.target.assign_result_to is not None:
            event.variable_map[event.target.assign_result_to] = result

        new_events = event.propogate(key_stack, result)
        if isinstance(new_events, EventResult):
            logger.debug(f"FlinkStatelessOperator {self.operator.dataflow.name}[{event._id}]: Returned {new_events}")
            yield new_events
        else:
            logger.debug(f"FlinkStatelessOperator {self.operator.dataflow.name}[{event._id}]: Propogated {len(new_events)} new Events")
            yield from new_events

class FlinkSelectAllOperator(KeyedProcessFunction):
    """A process function that yields all keys of a certain class"""
    def __init__(self):
        self.state: ValueState = None # type: ignore (expect state to be initialised on .open())
    
    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("entity-keys", Types.PICKLED_BYTE_ARRAY()) #,Types.OBJECT_ARRAY(Types.STRING()))
        self.state: ValueState = runtime_context.get_state(descriptor)

    def process_element(self, event: Event, ctx: 'ProcessFunction.Context'):
        state: list[str] = self.state.value()
        if state is None:
            state = []

        if isinstance(event.target, FlinkRegisterKeyNode):
            logger.debug(f"SelectAllOperator [{event.target.cls.__name__}]: Registering key: {event.target.key}")
            
            state.append(event.target.key)
            self.state.update(state)

        elif isinstance(event.target, SelectAllNode):
            logger.debug(f"SelectAllOperator [{event.target.cls.__name__}]: Selecting all")

            # Yield all the keys we now about
            event.key_stack.append(state)
            num_events = len(state)
            
            # Propogate the event to the next node
            new_events = event.propogate(event.key_stack, None)
            assert num_events == len(new_events)
            
            logger.debug(f"SelectAllOperator [{event.target.cls.__name__}]: Propogated {num_events} events with target: {event.target.collect_target}")
            yield from new_events
        else:
            raise Exception(f"Unexpected target for SelectAllOperator: {event.target}")

class FlinkCollectOperator(KeyedProcessFunction):
    """Flink implementation of a merge operator."""
    def __init__(self): #, merge_node: MergeNode) -> None:
        self.collection: ValueState = None # type: ignore (expect state to be initialised on .open())
        #self.node = merge_node

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("merge_state", Types.PICKLED_BYTE_ARRAY())
        self.collection = runtime_context.get_state(descriptor)

    def process_element(self, event: Event, ctx: KeyedProcessFunction.Context):
        collection: list[Result] = self.collection.value()
        logger.debug(f"FlinkCollectOp [{ctx.get_current_key()}]: Processing: {event}")
        
        # for now we assume there is only 1 merge per df
        assert event.collect_target is not None
        entry: CollectTarget = event.collect_target
        target_node: CollectNode = entry.target_node

        # Add to the map
        if collection == None:
            logger.debug(f"FlinkCollectOp [{ctx.get_current_key()}]: Creating map")
            collection = [NotArrived()] * entry.total_items
        logger.debug(f"FlinkCollectOp [{ctx.get_current_key()}]: Processed event {entry.result_idx} ({entry.total_items})")
        
        result = None
        try:
            result = event.variable_map[target_node.read_results_from]
        except KeyError:
            pass

        collection[entry.result_idx] = Arrived(result)
        self.collection.update(collection)
        
        # Yield events if the merge is done
        if all([isinstance(r, Arrived) for r in collection]):
            logger.debug(f"FlinkCollectOp [{ctx.get_current_key()}]: Yielding collection")
            
            collection = [r.val for r in collection] # type: ignore (r is of type Arrived)
            event.variable_map[target_node.assign_result_to] = collection
            new_events = event.propogate(event.key_stack, collection)

            self.collection.clear() 
            if isinstance(new_events, EventResult):
                logger.debug(f"FlinkCollectOp [{ctx.get_current_key()}]: Returned {new_events}")
                yield new_events
            else:
                logger.debug(f"FlinkCollectOp [{ctx.get_current_key()}]: Propogated {len(new_events)} new Events")
                yield from new_events

class FlinkMergeOperator(KeyedProcessFunction):
    """Flink implementation of a merge operator."""
    def __init__(self) -> None:
        self.other: ValueState = None # type: ignore (expect state to be initialised on .open())
    
    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("merge_state", Types.PICKLED_BYTE_ARRAY())
        self.other = runtime_context.get_state(descriptor)

    def process_element(self, event: Event, ctx: KeyedProcessFunction.Context):
        other_map = self.other.value()
        logger.debug(f"FlinkMergeOp [{ctx.get_current_key()}]: Processing: {event}")
        if other_map == None:
            logger.debug(f"FlinkMergeOp [{ctx.get_current_key()}]: Saving variable map")
            self.other.update(event.variable_map)
        else:
            self.other.clear() 
            logger.debug(f"FlinkMergeOp [{ctx.get_current_key()}]: Yielding merged variables")
            event.variable_map |= other_map
            new_event = event.propogate(event.key_stack, None)
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


class FlinkRuntime():
    """A Runtime that runs Dataflows on Flink."""
    def __init__(self, topic="input-topic", ui_port: Optional[int] = None):
        self.env: Optional[StreamExecutionEnvironment] = None
        """@private"""

        self.producer: Producer = None
        """@private"""

        self.sent_events = 0
        """The number of events that were sent using `send()`."""

        self.topic = topic
        """The topic to use for internal communications.

        Useful for running multiple instances concurrently, for example during 
        tests.
        """

        self.ui_port = ui_port
        """The port to run the Flink web UI on.

        Warning that this does not work well with run(collect=True)!"""

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
        logger.debug("FlinkRuntime initializing...")
        config = Configuration()
        # Add the Flink Web UI at http://localhost:8081
        if self.ui_port is not None:
            config.set_string("rest.port", str(self.ui_port))
        config.set_integer("python.fn-execution.bundle.time", bundle_time)
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
        kafka_source = (
            KafkaSource.builder()
                .set_bootstrap_servers(kafka_broker)
                .set_topics(self.topic)
                .set_group_id("test_group_1")
                .set_starting_offsets(KafkaOffsetsInitializer.earliest())
                .set_value_only_deserializer(deserialization_schema)
                .build()
        )
        self.kafka_internal_sink = (
            KafkaSink.builder()
                .set_bootstrap_servers(kafka_broker)
                .set_record_serializer(
                    KafkaRecordSerializationSchema.builder()
                        .set_topic(self.topic)
                        .set_value_serialization_schema(deserialization_schema)
                        .build()
                    )
                .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build()
        )
        """Kafka sink that will be ingested again by the Flink runtime."""

        event_stream = (
            self.env.from_source(
                    kafka_source, 
                    WatermarkStrategy.no_watermarks(),
                    "Kafka Source"
                )
                .map(lambda x: pickle.loads(x))
                .name("DESERIALIZE")
                # .filter(lambda e: isinstance(e, Event)) # Enforced by `send` type safety
            )
             
        # Events with a `SelectAllNode` will first be processed by the select 
        # all operator, which will send out multiple other Events that can
        # then be processed by operators in the same steam.
        select_all_stream = (
            event_stream.filter(lambda e: 
                    isinstance(e.target, SelectAllNode) or isinstance(e.target, FlinkRegisterKeyNode))
                .key_by(lambda e: e.target.cls)
                .process(FlinkSelectAllOperator()).name("SELECT ALL OP")
        )
        """Stream that ingests events with an `SelectAllNode` or `FlinkRegisterKeyNode`"""
        not_select_all_stream = (
            event_stream.filter(lambda e:
                    not (isinstance(e.target, SelectAllNode) or isinstance(e.target, FlinkRegisterKeyNode)))
        )

        event_stream_2 = select_all_stream.union(not_select_all_stream)

        operator_stream = event_stream_2.filter(lambda e: isinstance(e.target, OpNode)).name("OPERATOR STREAM")

        self.stateful_op_stream = (
            operator_stream
                .filter(lambda e: isinstance(e.target.operator, StatefulOperator))
        )

        self.stateless_op_stream = (
            operator_stream
                .filter(lambda e: isinstance(e.target.operator, StatelessOperator))
        )

        # self.merge_op_stream = (
        #     event_stream.filter(lambda e: isinstance(e.target, MergeNode))
        #         .key_by(lambda e: e._id) # might not work in the future if we have multiple merges in one dataflow?
        #         .process(FlinkMergeOperator())
        # )
        self.merge_op_stream = (
            event_stream.filter(lambda e: isinstance(e.target, CollectNode))
                .key_by(lambda e: e._id) # might not work in the future if we have multiple merges in one dataflow?
                .process(FlinkCollectOperator())
                .name("Collect")
        )
        """Stream that ingests events with an `cascade.dataflow.dataflow.MergeNode` target"""

        self.stateless_op_streams = []
        self.stateful_op_streams = []
        """List of stateful operator streams, which gets appended at `add_operator`."""

        self.producer = Producer({'bootstrap.servers': kafka_broker})
        logger.debug("FlinkRuntime initialized")
    
    def add_operator(self, flink_op: FlinkOperator):
        """Add a `FlinkOperator` to the Flink datastream."""
       
        op_stream = (
            self.stateful_op_stream.filter(lambda e: e.target.operator.entity == flink_op.operator.entity)
                .key_by(lambda e: e.key_stack[-1])
                .process(flink_op)
                .name("STATEFUL OP: " + flink_op.operator.entity.__name__)
            )
        self.stateful_op_streams.append(op_stream)

    def add_stateless_operator(self, flink_op: FlinkStatelessOperator):
        """Add a `FlinkStatlessOperator` to the Flink datastream."""
       
        op_stream = (
            self.stateless_op_stream
                .process(flink_op)
                .name("STATELESS DATAFLOW: " + flink_op.operator.dataflow.name)
            )
        self.stateless_op_streams.append(op_stream)

    def send(self, event: Event, flush=False):
        """Send an event to the Kafka source.
        Once `run` has been called, the Flink runtime will start ingesting these 
        messages. Messages can always be sent after `init` is called - Flink 
        will continue ingesting messages after `run` is called asynchronously.
        """
        self.producer.produce(self.topic, value=pickle.dumps(event))
        if flush:
            self.producer.flush()
        self.sent_events += 1

    def run(self, run_async=False, collect=False) -> Union[CloseableIterator, None]:
        """Start ingesting and processing messages from the Kafka source.
        
        If `collect` is True then this will return a CloseableIterator over 
        `cascade.dataflow.dataflow.EventResult`s."""

        assert self.env is not None, "FlinkRuntime must first be initialised with `init()`."
        logger.debug("FlinkRuntime merging operator streams...")

        # Combine all the operator streams
        operator_streams = self.merge_op_stream.union(*self.stateful_op_streams).union(*self.stateless_op_streams)
        
        # Add filtering for nodes with a `Filter` target
        full_stream_filtered = (
            operator_streams
                .filter(lambda e: isinstance(e, Event) and isinstance(e.target, Filter))
                .filter(lambda e: e.target.filter_fn())    
        )
        full_stream_unfiltered = (
            operator_streams
                .filter(lambda e: not (isinstance(e, Event) and isinstance(e.target, Filter)))
        )
        ds = full_stream_filtered.union(full_stream_unfiltered)

        # Output the stream
        if collect:
            ds_external = ds.filter(lambda e: isinstance(e, EventResult)).execute_and_collect() 
        else:
            ds_external = ds.filter(lambda e: isinstance(e, EventResult)).print() #.add_sink(kafka_external_sink) 
        ds_internal = ds.filter(lambda e: isinstance(e, Event)).sink_to(self.kafka_internal_sink).name("INTERNAL KAFKA SINK")

        if run_async:
            logger.debug("FlinkRuntime starting (async)")
            self.env.execute_async("Cascade: Flink Runtime")
            return ds_external # type: ignore (will be CloseableIterator provided the source is unbounded (i.e. Kafka))
        else:
            logger.debug("FlinkRuntime starting (sync)")
            self.env.execute("Cascade: Flink Runtime")