from abc import ABC
from dataclasses import dataclass
import os
import time
import uuid
import threading
from typing import Any, Literal, Optional, Type, Union
from pyflink.common.typeinfo import Types, get_gateway
from pyflink.common import Configuration, DeserializationSchema, SerializationSchema, WatermarkStrategy
from pyflink.datastream.data_stream import CloseableIterator
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, ValueState, ValueStateDescriptor
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSource, KafkaSink
from pyflink.datastream import ProcessFunction, StreamExecutionEnvironment
from pyflink.datastream.output_tag import OutputTag
import pickle 
from cascade.dataflow.dataflow import CallLocal, CollectNode, DataFlow, DataflowRef, Event, EventResult, InitClass, InvokeMethod, Node
from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from confluent_kafka import Producer, Consumer
import logging

logger = logging.getLogger("cascade")
logger.setLevel("INFO")
console_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Required if SelectAll nodes are used
SELECT_ALL_ENABLED = False

# Add profiling information to metadata
PROFILE = False

# Enable latency metrics
METRICS = False

@dataclass
class FlinkRegisterKeyNode(Node):
    """A node that will register a key with the SelectAll operator.
    
    This node is specific to Flink, and will be automatically generated.
    It should not be used in a `DataFlow`.
    
    @private
    """
    key: str
    cls: Type

    def propogate(self, event: Event, targets: list[Node], result: Any, **kwargs) -> list[Event]:
        """A key registration event does not propogate."""
        return []

class FanOutOperator(ProcessFunction):
    """"""
    def __init__(self, stateful_ops: dict[str, OutputTag], stateless_ops: dict[str, OutputTag]) -> None:
        self.stateful_ops = stateful_ops
        self.stateless_ops = stateless_ops

    def process_element(self, event: Event, ctx: ProcessFunction.Context):
        event = profile_event(event, "FanOut")

        logger.debug(f"FanOut Event entered: {event._id}")

        if isinstance(event.target, CallLocal):
            if event.dataflow.operator_name in self.stateful_ops:
                tag = self.stateful_ops[event.dataflow.operator_name]
            else:
                tag = self.stateless_ops[event.dataflow.operator_name]

        else:
            logger.error(f"FanOut: Wrong target: {event}")
            return
        
        logger.debug(f"Fanout Event routed to: {tag.tag_id}")
        yield tag, event

class RouterOperator(ProcessFunction):
    """Takes in an Event and Result as tuple. Calls Event.propogate on the event.

    The main output contains Events to be reingested into the system.
    There are two side outputs:
        - one for Events with a CollectNode target
        - one for EventResults
    """
    def __init__(self, dataflows: dict['DataflowRef', 'DataFlow'], collect_tag: OutputTag, out_tag: OutputTag) -> None:
        self.dataflows = dataflows
        self.collect_tag = collect_tag
        self.out_tag = out_tag

    def process_element(self, event_result: tuple[Event, Any], ctx: ProcessFunction.Context):
        event, result = event_result
        event = profile_event(event, "Router")

        logger.debug(f"RouterOperator Event entered: {event}")

        new_events = list(event.propogate(result, self.dataflows))
        
        if len(new_events) == 1 and isinstance(new_events[0], EventResult):
            logger.debug(f"RouterOperator: Returned {new_events[0]}")
        else:
            logger.debug(f"RouterOperator: Propogated {len(new_events)} new Events")
            for i, event in enumerate(new_events):
                logger.debug(f"{event} {i+1}/{len(new_events)}")
        
        for event in new_events:
            if isinstance(event, Event):
                if isinstance(event.target, CollectNode):
                    yield self.collect_tag, event
                else:
                    yield event
            else:
                assert isinstance(event, EventResult)
                yield self.out_tag, event

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
        event = profile_event(event, "STATEFUL OP INNER ENTRY")

        # should be handled by filters on this FlinkOperator    
        assert(isinstance(event.target, CallLocal)) 
        logger.debug(f"FlinkOperator {self.operator.name()}[{ctx.get_current_key()}]: Processing: {event.target.method}")
        
        assert(event.dataflow.operator_name == self.operator.name()) 
        key = ctx.get_current_key()
        assert(key is not None)

        if isinstance(event.target.method, InitClass):
            result = self.operator.handle_init_class(**event.variable_map).__dict__

            # Register the created key in FlinkSelectAllOperator
            if SELECT_ALL_ENABLED:
                register_key_event = Event(
                    FlinkRegisterKeyNode(key, self.operator.entity),
                    {},
                    None,
                    _id = event._id
                )
                logger.debug(f"FlinkOperator {self.operator.name()}[{ctx.get_current_key()}]: Registering key: {register_key_event}")
                yield register_key_event

            self.state.update(pickle.dumps(result))

        elif isinstance(event.target.method, InvokeMethod):
            state = self.state.value()
            if state is None:
                logger.error(f"FlinkOperator {self.operator.name()}[{ctx.get_current_key()}]: State does not exist for key {ctx.get_current_key()}")
                # raise KeyError(f"FlinkOperator {self.operator.name()}[{ctx.get_current_key()}]: State does not exist for key {ctx.get_current_key()}")
                # try to create it anyway
                state = self.operator.handle_init_class(*event.variable_map).__dict__
            else:
                state = pickle.loads(state)

            result = self.operator.handle_invoke_method(event.target.method, variable_map=event.variable_map, state=state)
            
            # TODO: check if state actually needs to be updated
            if state is not None:
                self.state.update(pickle.dumps(state))
        # Filter targets are used in cases of [hotel for hotel in Hotel.__all__() *if hotel....*]
        # elif isinstance(event.target.method_type, Filter):
        #     state = pickle.loads(self.state.value())
        #     result = event.target.method_type.filter_fn(event.variable_map, state)
        #     if not result:
        #         return
        #     result = event.key_stack[-1] 
        
        # if event.target.assign_result_to is not None:
        #     event.variable_map[event.target.assign_result_to] = result

        # new_events = list(event.propogate(result, self.operator.dataflows))
        
        # if len(new_events) == 1 and isinstance(new_events[0], EventResult):
        #     logger.debug(f"FlinkOperator {self.operator.name()}[{ctx.get_current_key()}]: Returned {new_events[0]}")
        # else:
        #     logger.debug(f"FlinkOperator {self.operator.name()}[{ctx.get_current_key()}]: Propogated {len(new_events)} new Events")
        
        # yield from new_events
        yield (event, result)

class FlinkStatelessOperator(ProcessFunction):
    """Wraps an `cascade.dataflow.datflow.StatefulOperator` in a KeyedProcessFunction so that it can run in Flink.
    """
    def __init__(self, operator: StatelessOperator) -> None:
        self.state: ValueState = None # type: ignore (expect state to be initialised on .open())
        self.operator = operator


    def process_element(self, event: Event, ctx: ProcessFunction.Context):
        event = profile_event(event, "STATELESS OP INNER ENTRY")

        assert isinstance(event.target, CallLocal)

        logger.debug(f"FlinkStatelessOperator {self.operator.name()}[{event._id}]: Processing: {event.target.method}")
        if isinstance(event.target.method, InvokeMethod):
            result = self.operator.handle_invoke_method(event.target.method, variable_map=event.variable_map)  
        else:
            raise Exception(f"A StatelessOperator cannot compute event type: {event.target.method}")
        

        # new_events = list(event.propogate(result, self.operator.dataflows))
        
        # if len(new_events) == 1 and isinstance(new_events[0], EventResult):
        #     logger.debug(f"FlinkStatelessOperator {self.operator.name()}[{event._id}]: Returned {new_events[0]}")
        # else:
        #     logger.debug(f"FlinkStatelessOperator {self.operator.name()}[{event._id}]: Propogated {len(new_events)} new Events")
        
        # yield from new_events
        yield (event, result)


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
            new_keys = state
            num_events = len(state)
            
            # Propogate the event to the next node
            new_events = event.propogate(None, select_all_keys=new_keys)
            assert isinstance(new_events, list), "SelectAll nodes shouldn't directly produce EventResults"
            assert num_events == len(new_events)
            
            logger.debug(f"SelectAllOperator [{event.target.cls.__name__}]: Propogated {num_events} events with target: {event.target.collect_target}")
            yield from new_events
        else:
            raise Exception(f"Unexpected target for SelectAllOperator: {event.target}")

class FlinkCollectOperator(KeyedProcessFunction):
    """Flink implementation of a merge operator."""
    def __init__(self): 
        self.collection: ValueState = None # type: ignore (expect state to be initialised on .open())

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor("merge_state", Types.PICKLED_BYTE_ARRAY())
        self.var_map = runtime_context.get_state(descriptor)

    def process_element(self, event: Event, ctx: KeyedProcessFunction.Context):
        event = profile_event(event, "COLLECT OP INNER ENTRY")

        var_map_num_items = self.var_map.value()
        logger.debug(f"FlinkCollectOp [{ctx.get_current_key()}]: Processing: {event}")

        assert isinstance(event.target, CollectNode)
        
        total_events = event.target.num_events

        # Add to the map
        if var_map_num_items == None:
            logger.debug(f"FlinkCollectOp [{ctx.get_current_key()}]: Creating map")
            combined_var_map = {}
            num_items = 0
        else:
            combined_var_map, num_items = var_map_num_items

        combined_var_map.update(event.variable_map)
        num_items += 1
        logger.debug(f"FlinkCollectOp [{ctx.get_current_key()}]: Recieved {num_items}/{total_events} Events")

        
        if num_items == total_events:
            logger.debug(f"FlinkCollectOp [{ctx.get_current_key()}]: Yielding collection")
            event.variable_map = combined_var_map
            # yield from event.propogate(None)
            yield (event, None)
            self.var_map.clear()
        else:
            self.var_map.update((combined_var_map, num_items))


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

def deserialize_and_timestamp(x) -> Event:
    t1 = time.time()
    e: Event = pickle.loads(x)
    t2 = time.time()
    if e.metadata["in_t"] is None:
        e.metadata["in_t"] = t1
    e.metadata["current_in_t"] = t1
    e.metadata["deser_times"].append(t2 - t1)
    return e

def timestamp_event(e: Event) -> Event:
    t1 = time.time()
    try:
        e.metadata["flink_time"] += t1 - e.metadata["current_in_t"]
    except KeyError:
        pass
    return e

def profile_event(e: Event, ts_name: str) -> Event:
    if not PROFILE:
        return e
    t1 = time.time()
    if "prof" not in e.metadata:
        e.metadata["prof"] = []
    e.metadata["prof"].append((ts_name, t1))
    return e

def timestamp_result(e: EventResult) -> EventResult:
    t1 = time.time()
    e.metadata["out_t"] = t1
    e.metadata["flink_time"] += t1 - e.metadata["current_in_t"] 
    e.metadata["loops"] = len(e.metadata["deser_times"])
    e.metadata["roundtrip"] = e.metadata["out_t"] - e.metadata["in_t"]
    return e

def debug(x, msg=""):
    logger.debug(msg)
    return x

class FlinkRuntime():
    """A Runtime that runs Dataflows on Flink."""
    def __init__(self, input_topic="input-topic", output_topic="output-topic", ui_port: Optional[int] = None, internal_topic="internal-topic"):
        self.env: Optional[StreamExecutionEnvironment] = None
        """@private"""

        self.sent_events = 0
        """The number of events that were sent using `send()`."""

        self.input_topic = input_topic
        """The topic to use read new events/requests."""

        self.internal_topic = internal_topic
        """The topic used for internal messages."""

        self.output_topic = output_topic
        """The topic to use for external communications, i.e. when a dataflow is
        finished. As such only `EventResult`s should be contained in this topic.
        """

        self.ui_port = ui_port
        """The port to run the Flink web UI on.

        Warning that this does not work well with run(collect=True)!"""

        self.stateless_operators: list[FlinkStatelessOperator] = []
        self.stateful_operators: list[FlinkOperator] = []
        """List of stateful operator streams, which gets appended at `add_operator`."""

        self.dataflows: dict['DataflowRef', 'DataFlow'] = {}


    def init(self, kafka_broker="localhost:9092", bundle_time=1, bundle_size=5, parallelism=None, thread_mode=False):
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
        
        # Thread mode has significant performance impacts, see
        # https://flink.apache.org/2022/05/06/exploring-the-thread-mode-in-pyflink/
        # In short:
        #   much faster single threaded python performance
        #   GIL becomes an issue if running higher parallelism on the same taskmanager
        #   can't use with minicluster (e.g. while testing)
        if thread_mode:
            config.set_string("python.execution-mode", "thread")

        # METRICS
        if METRICS:
            config.set_boolean("python.metric.enabled", True)
            config.set_string("metrics.latency.interval", "500 ms")
            config.set_boolean("state.latency-track.keyed-state-enabled", True)
            config.set_boolean("taskmanager.network.detailed-metrics", True)

        # optimize for low latency
        config.set_string("execution.batch-shuffle-mode", "ALL_EXCHANGES_PIPELINED")
        config.set_string("execution.buffer-timeout", "0 ms")
        
        
        kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
        'bin/flink-sql-connector-kafka-3.3.0-1.20.jar')
        serializer_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'bin/flink-kafka-bytes-serializer.jar')
        flink_jar = "/home/lvanmol/flink-1.20.1/opt/flink-python-1.20.1.jar"
        
        # Add the required jars https://issues.apache.org/jira/browse/FLINK-36457q
        config.set_string("pipeline.jars",f"file://{flink_jar};file://{kafka_jar};file://{serializer_jar}")

        self.env = StreamExecutionEnvironment.get_execution_environment(config)
        if parallelism:
            self.env.set_parallelism(parallelism)
        parallelism = self.env.get_parallelism()

        logger.info(f"FlinkRuntime: parallelism {parallelism}")


        deserialization_schema = ByteSerializer()

        kafka_external_source = (
            KafkaSource.builder()
                .set_bootstrap_servers(kafka_broker)
                .set_topics(self.input_topic)
                .set_group_id("test_group_1")
                .set_starting_offsets(KafkaOffsetsInitializer.earliest())
                .set_value_only_deserializer(deserialization_schema)
                .build()
        )
        kafka_internal_source = (
            KafkaSource.builder()
                .set_bootstrap_servers(kafka_broker)
                .set_topics(self.internal_topic)
                .set_group_id("test_group_1")
                .set_starting_offsets(KafkaOffsetsInitializer.earliest())
                .set_value_only_deserializer(deserialization_schema)
                .set_property("fetch.min.bytes", "1")
                .set_property("max.partition.fetch.bytes", "1048576")
                .set_property("enable.auto.commit", "false")
                .build()
        )
        self.kafka_internal_sink = (
            KafkaSink.builder()
                .set_bootstrap_servers(kafka_broker)
                .set_property("linger.ms", "0")
                .set_property("acks", "1")
                .set_record_serializer(
                    KafkaRecordSerializationSchema.builder()
                        .set_topic(self.internal_topic)
                        .set_value_serialization_schema(deserialization_schema)
                        .build()
                    )
                .build()
        )
        """Kafka sink that will be ingested again by the Flink runtime."""

        self.kafka_external_sink = (
            KafkaSink.builder()
                .set_bootstrap_servers(kafka_broker)
                .set_record_serializer(
                    KafkaRecordSerializationSchema.builder()
                        .set_topic(self.output_topic)
                        .set_value_serialization_schema(deserialization_schema)
                        .build()
                    )
                .build()
        )
        """Kafka sink corresponding to outputs of calls (`EventResult`s)."""

        event_stream = (
                self.env.from_source(
                    kafka_external_source, 
                    WatermarkStrategy.no_watermarks(),
                    "Kafka External Source"
                )
                .map(lambda x: deserialize_and_timestamp(x))
                .set_parallelism(parallelism=max(parallelism//4, 1))
                .name("DESERIALIZE external")
                # .filter(lambda e: isinstance(e, Event)) # Enforced by `send` type safety
            ).union(
                self.env.from_source(
                    kafka_internal_source, 
                    WatermarkStrategy.no_watermarks(),
                    "Kafka Internal Source"
                )
                .map(lambda x: deserialize_and_timestamp(x))
                .name("DESERIALIZE internal")
            )#.map(lambda e: profile_event(e, "DESERIALIZE DONE"))

        # Events with a `SelectAllNode` will first be processed by the select 
        # all operator, which will send out multiple other Events that can
        # then be processed by operators in the same steam.
        if SELECT_ALL_ENABLED:
            select_all_stream = (
                event_stream.filter(lambda e: 
                        isinstance(e.target, SelectAllNode) or isinstance(e.target, FlinkRegisterKeyNode))
                    .key_by(lambda e: e.target.cls)
                    .process(FlinkSelectAllOperator()).name("SELECT ALL OP")
            )
            # Stream that ingests events with an `SelectAllNode` or `FlinkRegisterKeyNode`
            not_select_all_stream = (
                event_stream.filter(lambda e:
                        not (isinstance(e.target, SelectAllNode) or isinstance(e.target, FlinkRegisterKeyNode)))
            )

            event_stream = select_all_stream.union(not_select_all_stream)

        # # event_stream = event_stream.disable_chaining()
        # self.stateful_op_stream = event_stream
        # self.stateless_op_stream = event_stream
        self.event_stream = event_stream


        logger.debug("FlinkRuntime initialized")
    
    def add_operator(self, op: StatefulOperator):
        """Add a `FlinkOperator` to the Flink datastream."""
        flink_op = FlinkOperator(op)

        self.stateful_operators.append(flink_op)
        self.dataflows.update(op.dataflows)

    def add_stateless_operator(self, op: StatelessOperator):
        """Add a `FlinkStatelessOperator` to the Flink datastream."""
        flink_op = FlinkStatelessOperator(op)

        self.stateless_operators.append(flink_op)
        self.dataflows.update(op.dataflows)

    def add_dataflow(self, dataflow: DataFlow):
        """When adding extra dataflows, e.g. when testing or for optimized versions"""
        self.dataflows[dataflow.ref()] = dataflow


    def run(self, run_async=False, output: Literal["collect", "kafka", "stdout"]="kafka") -> Union[CloseableIterator, None]:
        """Start ingesting and processing messages from the Kafka source.
        
        If `collect` is True then this will return a CloseableIterator over 
        `cascade.dataflow.dataflow.EventResult`s."""

        assert self.env is not None, "FlinkRuntime must first be initialised with `init()`."
        logger.info("FlinkRuntime merging operator streams...")

        # create the fanout operator
        stateful_tags = { op.operator.name() : OutputTag(op.operator.name()) for op in self.stateful_operators}
        stateless_tags = { op.operator.name() : OutputTag(op.operator.name()) for op in self.stateless_operators}
        collect_tag = OutputTag("__COLLECT__")
        result_tag = OutputTag("__EVENT_RESULT__")
        logger.debug(f"Stateful tags: {stateful_tags.items()}")
        logger.debug(f"Stateless tags: {stateless_tags.items()}")
        fanout = self.event_stream.process(FanOutOperator(stateful_tags, stateless_tags)).name("FANOUT OPERATOR")#.disable_chaining()

        # create the streams
        self.stateful_op_streams = []
        for flink_op in self.stateful_operators:
            tag = stateful_tags[flink_op.operator.name()]
            op_stream = (
                fanout
                    .get_side_output(tag)
                    .key_by(lambda e: e.key)
                    .process(flink_op)
                    .name("STATEFUL OP: " + flink_op.operator.name())
            )
            self.stateful_op_streams.append(op_stream)

        self.stateless_op_streams = []
        for flink_op in self.stateless_operators:
            tag = stateless_tags[flink_op.operator.name()]
            op_stream = (
                fanout
                    .get_side_output(tag)
                    .process(flink_op)
                    .name("STATELESS OP: " + flink_op.operator.name())
            )
            self.stateless_op_streams.append(op_stream)

        # Combine all the operator streams
        if len(self.stateful_op_streams) >= 1:
            s1 = self.stateful_op_streams[0]
            rest = self.stateful_op_streams[1:]
            operator_streams = s1.union(*rest, *self.stateless_op_streams)#.map(lambda e: profile_event(e, "OP STREAM UNION"))
        elif len(self.stateless_op_streams) >= 1:
            s1 = self.stateless_op_streams[0]
            rest = self.stateless_op_streams[1:]
            operator_streams = s1.union(*rest, *self.stateful_op_streams)#.map(lambda e: profile_event(e, "OP STREAM UNION"))
        else:
            raise RuntimeError("No operators found, were they added to the flink runtime with .add_*_operator()")


        op_routed = operator_streams.process(RouterOperator(self.dataflows, collect_tag, result_tag)).name("ROUTER (OP)")

        collect_stream = (
            op_routed
                .get_side_output(collect_tag)
                .key_by(lambda e: str(e._id) + "_" + str(e.target.id)) # might not work in the future if we have multiple merges in one dataflow?
                .process(FlinkCollectOperator())
                .name("Collect")
                .process(RouterOperator(self.dataflows, collect_tag, result_tag))
        )
        """Stream that ingests events with an `cascade.dataflow.dataflow.CollectNode` target"""

        # descriptor = ValueStateDescriptor("dataflows", Types.PICKLED_BYTE_ARRAY())

        # broadcast_dataflows = self.env.broadcast_variable("dataflows", list(self.dataflows.items()))
        # union with EventResults or Events that don't have a CollectNode target


        # Output the stream
        results = (
            op_routed.get_side_output(result_tag).union(collect_stream.get_side_output(result_tag))
                # .filter(lambda e: isinstance(e, EventResult))
                # .map(lambda e: profile_event(e, "EXTERNAL SINK"))
                .map(lambda e: timestamp_result(e))
        )
        if output == "collect":
            ds_external = results.execute_and_collect() 
        elif output == "stdout":
            ds_external = results.print()
        elif output == "kafka":
            ds_external = results.sink_to(self.kafka_external_sink).name("EXTERNAL KAFKA SINK") 
        else:
            raise ValueError(f"Invalid output: {output}")

        ds_internal = (
            op_routed.union(collect_stream)
                # .filter(lambda e: isinstance(e, Event))
                # .map(lambda e: profile_event(e, "INTERNAL SINK"))
                .map(lambda e: timestamp_event(e))
                .sink_to(self.kafka_internal_sink)
                .name("INTERNAL KAFKA SINK")
        )

        if run_async:
            logger.info("FlinkRuntime starting (async)")
            self.env.execute_async("Cascade: Flink Runtime")
            return ds_external # type: ignore (will be CloseableIterator provided the source is unbounded (i.e. Kafka))
        else:
            logger.info("FlinkRuntime starting (sync)")
            self.env.execute("Cascade: Flink Runtime")
    
    def close(self):
        assert self.env is not None, "FlinkRuntime must first be initialised with `init()`."
        self.env.close()

class FlinkClientSync:
    def __init__(self, input_topic="input-topic", output_topic="output-topic", kafka_url="localhost:9092", start_consumer_thread: bool = True):
        self.producer = Producer({'bootstrap.servers': kafka_url})
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.kafka_url = kafka_url
        self.is_consuming = False
        self._futures: dict[int, dict] = {} # TODO: handle timeouts?
        """Mapping of event id's to their EventResult. None if not arrived."""
        if start_consumer_thread:
            self.start_consumer_thread()      

    def start_consumer_thread(self):
        self.result_consumer_process = threading.Thread(target=self.consume_results)
        self.is_consuming = True
        self.result_consumer_process.start()

    def consume_results(self):
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.kafka_url, 
                "group.id": str(uuid.uuid4()), 
                "auto.offset.reset": "earliest",
                "api.version.request": True
                # "enable.auto.commit": False,
                # "fetch.min.bytes": 1
            })
        
        self.consumer.subscribe([self.output_topic])

        while self.is_consuming:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            try:
                event_result: EventResult = pickle.loads(msg.value())
                ts = msg.timestamp()
                if event_result.event_id in self._futures:
                    if (r := self._futures[event_result.event_id]["ret"]) != None:
                        logger.warning(f"Recieved EventResult with id {event_result.event_id} more than once: {event_result} replaced previous: {r}")
                    self._futures[event_result.event_id]["ret"] = event_result
                    self._futures[event_result.event_id]["ret_t"] = ts
            except Exception as e:
                logger.error(f"Consumer deserializing error: {e}")
                
        
        self.consumer.close()

    def flush(self):
        self.producer.flush()

    def send(self, event: Union[Event, list[Event]], flush=False, block=False) -> int:
        if isinstance(event, list):
            for e in event:
                id = self._send(e)
        else:
            id = self._send(event)

        if flush or block:
            self.producer.flush()

        if block:
            while (r := self._futures[id]["ret"]) == None:
                time.sleep(0.1)

            return r.result
        else:
            return id


    def _send(self, event: Event) -> int:
        """Send an event to the Kafka source and block until an EventResult is recieved.

        :param event: The event to send.
        :param flush: Whether to flush the producer after sending.
        :return: The result event if recieved
        :raises Exception: If an exception occured recieved or deserializing the message.
        """
        self._futures[event._id] = {
            "sent": event,
            "sent_t": None,
            "ret": None,
            "ret_t": None
        }

        def set_ts(ts):
            self._futures[event._id]["sent_t"] = ts

        self.producer.produce(self.input_topic, value=pickle.dumps(event), on_delivery=lambda err, msg: set_ts(msg.timestamp()))
        return event._id        

    def close(self):
        self.producer.flush()
        self.is_consuming = False
