from logging import Filter
import threading
from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from cascade.dataflow.dataflow import CollectNode, Event, EventResult, InitClass, InvokeMethod, OpNode, SelectAllNode
from queue import Empty, Queue

class PythonStatefulOperator():
    def __init__(self, operator: StatefulOperator):
        self.operator = operator
        self.states = {}
    
    def process(self, event: Event):
        assert(isinstance(event.target, OpNode))
        assert(isinstance(event.target.operator, StatefulOperator))
        assert(event.target.operator.entity == self.operator.entity)
        key_stack = event.key_stack
        key = key_stack[-1]

        print(f"PythonStatefulOperator: {event}")

        if isinstance(event.target.method_type, InitClass):
            result = self.operator.handle_init_class(*event.variable_map.values())
            self.states[key] = result
            key_stack.pop()

        elif isinstance(event.target.method_type, InvokeMethod):
            state = self.states[key]
            result = self.operator.handle_invoke_method(
                event.target.method_type, 
                variable_map=event.variable_map, 
                state=state, 
                key_stack=key_stack
            )
            self.states[key] = state
        
        elif isinstance(event.target.method_type, Filter):
            raise NotImplementedError()
    
        if event.target.assign_result_to is not None:
            event.variable_map[event.target.assign_result_to] = result
        
        new_events = event.propogate(key_stack, result)
        if isinstance(new_events, EventResult):
            yield new_events
        else:
            yield from new_events

class PythonStatelessOperator():
    def __init__(self, operator: StatelessOperator):
        self.operator = operator
    
    def process(self, event: Event):
        assert(isinstance(event.target, OpNode))
        assert(isinstance(event.target.operator, StatelessOperator))

        key_stack = event.key_stack


        if isinstance(event.target.method_type, InvokeMethod):
            result = self.operator.handle_invoke_method(
                event.target.method_type, 
                variable_map=event.variable_map, 
                key_stack=key_stack
            )
        else:
            raise Exception(f"A StatelessOperator cannot compute event type: {event.target.method_type}")
        
        if event.target.assign_result_to is not None:
            event.variable_map[event.target.assign_result_to] = result
        
        new_events = event.propogate(key_stack, result)
        if isinstance(new_events, EventResult):
            yield new_events
        else:
            yield from new_events


class PythonRuntime():
    """Simple non-distributed runtime meant for testing that runs Dataflows locally."""
    def __init__(self):
        self.events = Queue()
        self.results = Queue()
        self.running = False
        self.statefuloperators: dict[StatefulOperator, PythonStatefulOperator] = {}
        self.statelessoperators: dict[StatelessOperator, PythonStatelessOperator] = {}

    def init(self):
        pass

    def _consume_events(self):
        self.running = True
        def consume_event(event: Event):
            if isinstance(event.target, OpNode):
                if isinstance(event.target.operator, StatefulOperator):
                    yield from self.statefuloperators[event.target.operator].process(event)
                elif isinstance(event.target.operator, StatelessOperator):
                    yield from self.statelessoperators[event.target.operator].process(event)

            elif isinstance(event.target, SelectAllNode):
                raise NotImplementedError()
            elif isinstance(event.target, CollectNode):
                raise NotImplementedError()
            
    
        events = []
        while self.running:
            if len(events) == 0:
                try:
                    event: Event = self.events.get(timeout=1)
                except Empty:
                    continue
            else:
                event = events.pop()

            for ev in consume_event(event):
                if isinstance(ev, EventResult):
                    print(ev)
                    self.results.put(ev)
                elif isinstance(ev, Event):
                    events.append(ev)
    
    def add_operator(self, op: StatefulOperator):
        """Add a `StatefulOperator` to the datastream."""
        self.statefuloperators[op] = PythonStatefulOperator(op)

    def add_stateless_operator(self, op: StatelessOperator):
        """Add a `StatelessOperator` to the datastream."""
        self.statelessoperators[op] = PythonStatelessOperator(op)

    def send(self, event: Event, flush=None):
        self.events.put(event)

    def run(self):
        self.thread = threading.Thread(target=self._consume_events, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        self.thread.join()

class PythonClientSync:
    def __init__(self, runtime: PythonRuntime):
        self._results_q = runtime.results
        self._events = runtime.events
        self.results = {}
             
    def send(self, event: Event, block=True):
        self._events.put(event)

        while block:
            er: EventResult = self._results_q.get(block=True)
            if event._id == er.event_id:
                self.results[er.event_id] = er.result
                return er.result
    
        