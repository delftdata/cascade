import threading
from typing import List, Union
from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from cascade.dataflow.dataflow import CallEntity, CallLocal, CollectNode, Event, EventResult, InitClass, InvokeMethod, OpNode, StatelessOpNode
from queue import Empty, Queue

import time

class PythonStatefulOperator():
    def __init__(self, operator: StatefulOperator):
        self.operator = operator
        self.states = {}
    
    def process(self, event: Event):
        assert(isinstance(event.target, CallLocal))

        key = event.key

        print(f"PythonStatefulOperator[{self.operator.entity.__name__}[{key}]]: {event}")

        if isinstance(event.target.method, InitClass):
            result = self.operator.handle_init_class(*event.variable_map.values())
            self.states[key] = result

        elif isinstance(event.target.method, InvokeMethod):
            state = self.states[key]
            result = self.operator.handle_invoke_method(
                event.target.method, 
                variable_map=event.variable_map, 
                state=state, 
            )
            self.states[key] = state

        new_events = event.propogate(result)
        if isinstance(new_events, EventResult):
            yield new_events
        else:
            yield from new_events

class PythonStatelessOperator():
    def __init__(self, operator: StatelessOperator):
        self.operator = operator
    
    def process(self, event: Event):
        assert(isinstance(event.target, CallLocal))

        print(f"PythonStatelessOperator[{self.operator.name()}]: {event}")

        if isinstance(event.target.method, InvokeMethod):
            result = self.operator.handle_invoke_method(
                event.target.method, 
                variable_map=event.variable_map, 
            )
        else:
            raise Exception(f"A StatelessOperator cannot compute event type: {event.target.method}")
        
        new_events = event.propogate(result)
        if isinstance(new_events, EventResult):
            yield new_events
        else:
            yield from new_events

class PythonCollectOperator():
    def __init__(self):
        self.state = {}

    def process(self, event: Event):
        key = event.target.id
        if key not in self.state:
            self.state[key] = [event]
        else:
            self.state[key].append(event)

        n = len(event.dataflow.get_predecessors(event.target))
        print(f"PythonCollectOperator: collected {len(self.state[key])}/{n} for event {event._id}")

        if len(self.state[key]) == n:
            var_map = {}
            for event in self.state[key]:
                var_map.update(event.variable_map)

            event.variable_map = var_map
            new_events = event.propogate(None)
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
        self.statefuloperators: dict[str, PythonStatefulOperator] = {}
        self.statelessoperators: dict[str, PythonStatelessOperator] = {}
        self.collect = PythonCollectOperator()

    def init(self):
        pass

    def _consume_events(self):
        try:
            self._run()
        except Exception as e:
            self.running = False
            raise e

    def _run(self):
        self.running = True
        def consume_event(event: Event):
            if isinstance(event.target, CallLocal):
                if event.dataflow.op_name in self.statefuloperators:
                    yield from self.statefuloperators[event.dataflow.op_name].process(event)
                else:
                    yield from self.statelessoperators[event.dataflow.op_name].process(event)
            elif isinstance(event.target, CallEntity):
                new_events = event.propogate(None)
                if isinstance(new_events, EventResult):
                    yield new_events
                else:
                    yield from new_events

            elif isinstance(event.target, CollectNode):
                yield from self.collect.process(event)
            
    
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
                    self.results.put(ev)
                elif isinstance(ev, Event):
                    events.append(ev)
    
    def add_operator(self, op: StatefulOperator):
        """Add a `StatefulOperator` to the datastream."""
        self.statefuloperators[op.name()] = PythonStatefulOperator(op)

    def add_stateless_operator(self, op: StatelessOperator):
        """Add a `StatelessOperator` to the datastream."""
        self.statelessoperators[op.name()] = PythonStatelessOperator(op)

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
        self.runtime = runtime
             
    def send(self, event: Union[Event, List[Event]], block=True):
        if isinstance(event, list):
            for e in event:
                self._events.put(e)
                id = e._id
        else:       
            self._events.put(event)
            id = event._id

        while block and self.runtime.running:
            try:
                er: EventResult = self._results_q.get(block=False, timeout=0.1)
            except Empty:
                continue
            if id == er.event_id:
                self.results[er.event_id] = er.result
                return er.result
    
        