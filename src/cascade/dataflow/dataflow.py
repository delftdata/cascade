from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Mapping, Optional, Union
from typing import TYPE_CHECKING
import uuid

import cascade

if TYPE_CHECKING:
    from cascade.frontend.generator.local_block import CompiledLocalBlock
    from cascade.dataflow.operator import Operator


@dataclass
class InitClass:
    """A method type corresponding to an `__init__` call."""
    pass

@dataclass
class InvokeMethod:
    """A method invocation of the underlying method indentifier."""
    method_name: str

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.method_name}')"


@dataclass
class Node(ABC):
    """Base class for Nodes."""

    id: int = field(init=False)
    """This node's unique id."""

    _id_counter: int = field(init=False, default=0, repr=False)
    outgoing_edges: list['Edge'] = field(init=False, default_factory=list, repr=False)
    
    def __post_init__(self):
        # Assign a unique ID from the class-level counter
        self.id = Node._id_counter
        Node._id_counter += 1

    @abstractmethod
    def propogate(self, event: 'Event', targets: list['Node'], result: Any, df_map: dict['DataflowRef', 'DataFlow'], **kwargs) -> list['Event']:
        pass

@dataclass
class IfNode(Node):
    predicate_var: str

    def propogate(self, event: 'Event', targets: List[Node], result: Any, df_map: dict['DataflowRef', 'DataFlow'], **kwargs) -> List['Event']:
        
        if_cond = event.variable_map[self.predicate_var]
        targets = []
        for edge in event.target.outgoing_edges:
            assert edge.if_conditional is not None
            if edge.if_conditional == if_cond:
                targets.append(edge.to_node)


        events = []
        for target in targets:
            ev = Event(
                target,
                event.variable_map, 
                event.dataflow,
                call_stack=event.call_stack,
                _id=event._id,
                metadata=event.metadata,
                key=event.key)
            
            events.append(ev)
        return events
    
    def __str__(self) -> str:
        return f"IF {self.predicate_var}"

@dataclass
class DataflowRef:
    operator_name: str
    dataflow_name: str

    # def get_dataflow(self) -> 'DataFlow':
    #     try:
    #         return cascade.core.dataflows[self]
    #     except KeyError as e:
    #         raise KeyError(f"DataflowRef {self} not found in cascade.core.dataflows")
    
    def __repr__(self) -> str:
        return f"{self.operator_name}.{self.dataflow_name}"
    
    def __hash__(self) -> int:
        return hash(repr(self))
        

@dataclass
class CallRemote(Node):
    """A node in a `DataFlow` corresponding to the call of another dataflow"""
    dataflow: 'DataflowRef'
    """The dataflow to call."""

    variable_rename: dict[str, str]
    """A mapping of input variables (to the dataflow) to variables in the variable map"""

    assign_result_to: Optional[str] = None
    """What variable to assign the result of this node to, if any."""

    keyby: Optional[str] = None
    """The key, for calls to Stateful Entities"""

    def propogate(self, event: 'Event', targets: List[Node], result: Any, df_map: dict['DataflowRef', 'DataFlow']) -> List['Event']:
        # remap the variable map of event into the new event
        new_var_map = {key: event.variable_map[value] for key, value in self.variable_rename.items()}
        if self.keyby:
            new_key = event.variable_map[self.keyby]
        else:
            new_key = None
        df = df_map[self.dataflow]
        new_targets = df.entry

        # Tail call elimination:
        # "targets" corresponds to where to go after this CallRemote finishes
        # the call to self.dataflow
        #
        # If this CallRemote is a terminal node in event.dataflow, then we don't
        # need to go back to event.dataflow, so we don't add it to the call stack.
        # This node is terminal in event.dataflow iff len(targets) == 0
        new_call_stack = event.call_stack
        if len(targets) > 0:
            new_call_stack = event.call_stack.copy()
            call = CallStackItem(event.dataflow, self.assign_result_to, event.variable_map, targets, key=event.key)
            new_call_stack.append(call)

        return [Event(
                    target,
                    new_var_map, 
                    self.dataflow,
                    _id=event._id,
                    metadata=event.metadata,
                    call_stack=new_call_stack,
                    key=new_key)
                    
                    for target in new_targets]
    
    def __str__(self) -> str:
        return f"CALL {self.dataflow}"


@dataclass
class CallLocal(Node):
    method: Union[InvokeMethod, InitClass]

    def propogate(self, event: 'Event', targets: List[Node], result: Any, df_map: dict['DataflowRef', 'DataFlow'], **kwargs) -> List['Event']:
        # For simple calls, we only need to change the target.
        # Multiple targets results in multiple events
        events = []
        for target in targets:
            ev = Event(
                target,
                event.variable_map, 
                event.dataflow,
                call_stack=event.call_stack,
                _id=event._id,
                metadata=event.metadata,
                key=event.key)
            
            events.append(ev)
        return events

    def __str__(self) -> str:
        return f"LOCAL {self.method}"

@dataclass
class CollectNode(Node):
    """A node in a `Dataflow` corresponding to a merge operator. 
    
    It will aggregate incoming edges and output them as a list to the outgoing edge.
    Their actual implementation is runtime-dependent."""
    num_events: int

    def propogate(self, event: 'Event', targets: List[Node], result: Any, df_map: dict['DataflowRef', 'DataFlow'], **kwargs) -> List['Event']:
        return [Event(
                    target,
                    event.variable_map, 
                    event.dataflow,
                    _id=event._id,
                    call_stack=event.call_stack,
                    # collect_target=ct,
                    metadata=event.metadata,
                    key=event.key)
                    
                    for target in targets]
    
    def __str__(self) -> str:
        return f"COLLECT {self.num_events}"

@dataclass
class Edge():
    """An Edge in the Dataflow graph."""
    from_node: Node
    to_node: Node
    if_conditional: Optional[bool] = None

class DataFlow:
    """A Dataflow is a graph consisting of `OpNode`s, `MergeNode`s, and `Edge`s.
    
    Example Usage
    -------------

    Consider two entities, `User` and `Item`, and a method `User.buy_items(item1, item2)`.
    The resulting method could be created into the following Dataflow graph.

    ```mermaid
    flowchart TD;
        user1[User.buy_items_0]
        item1[Item.get_price]
        item2[Item.get_price]
        user2[User.buy_items_1]
        collect{Collect}
        user1-- item1_key -->item1;
        user1-- item2_key -->item2;
        item1-- item1_price -->collect;
        item2-- item2_price -->collect;
        collect-- [item1_price, item2_price] -->user2;
    ```
    """

    def __init__(self, name: str, op_name: str, args: Optional[list[str]]=None):
        self.name: str = name
        self.adjacency_list: dict[int, list[int]] = {}
        self.nodes: dict[int, Node] = {}
        self.edges: list[Edge] = []
        self.entry: List[Node] = []
        self.operator_name = op_name
        if args:
            self.args: list[str] = args
        else:
            self.args = []
        self.blocks: dict[str, 'CompiledLocalBlock'] = {}

    def ref(self) -> DataflowRef:
        return DataflowRef(self.operator_name, self.name)

    def add_node(self, node: Node):
        """Add a node to the Dataflow graph if it doesn't already exist."""
        if node.id not in self.adjacency_list:
            node.outgoing_edges = []
            self.adjacency_list[node.id] = []
            self.nodes[node.id] = node

    def add_block(self, block: 'CompiledLocalBlock'):
        self.blocks[block.get_method_name()] = block


    def add_edge(self, edge: Edge):
        """Add an edge to the Dataflow graph. Nodes that don't exist will be added to the graph automatically."""
        self.add_node(edge.from_node)
        self.add_node(edge.to_node)
        if edge.to_node.id not in self.adjacency_list[edge.from_node.id]:
            self.adjacency_list[edge.from_node.id].append(edge.to_node.id)
            edge.from_node.outgoing_edges.append(edge)
            self.edges.append(edge)

    def add_edge_refs(self, u: int, v: int, if_conditional=None):
        """Add an edge using node IDs"""
        from_node = self.nodes[u]
        to_node = self.nodes[v]
        self.add_edge(Edge(from_node, to_node, if_conditional=if_conditional))
    
    def remove_edge(self, from_node: Node, to_node: Node):
        """Remove an edge from the Dataflow graph."""
        if from_node.id in self.adjacency_list and to_node.id in self.adjacency_list[from_node.id]:
            # Remove from adjacency list
            self.adjacency_list[from_node.id].remove(to_node.id)
            # Remove from outgoing_edges
            from_node.outgoing_edges = [
                edge for edge in from_node.outgoing_edges if edge.to_node.id != to_node.id
            ]

            # TODO: replace self.edges with a better algorithm for removal. 
            # probably by adding edge information (like edge.if_conditional, or future things)
            # to self.adjacencylist
            for i, edge in enumerate(self.edges):
                if edge.from_node == from_node and edge.to_node == to_node:
                    break
            self.edges.pop(i)

    def remove_node(self, node: Node):
        return self.remove_node_by_id(node.id)

    def remove_node_by_id(self, node_id: int):
        """Remove a node from the DataFlow graph and reconnect its parents to its children."""
        if node_id not in self.nodes:
            return  # Node doesn't exist in the graph


        # Find parents (nodes that have edges pointing to this node)
        parents = [parent_id for parent_id, children in self.adjacency_list.items() if node_id in children]

        # Find children (nodes that this node points to)
        children = self.adjacency_list[node_id]
        
        # Set df entry
        if len(self.entry) == 1 and self.entry[0].id == node_id:
            assert len(children) <= 1, "cannot remove entry node if it has more than two children"
            self.entry = [self.nodes[id] for id in children]

        # Connect each parent to each child
        for parent_id in parents:
            parent_node = self.nodes[parent_id]
            for child_id in children:
                child_node = self.nodes[child_id]
                new_edge = Edge(parent_node, child_node)
                self.add_edge(new_edge)

        # Remove edges from parents to the node
        for parent_id in parents:
            parent_node = self.nodes[parent_id]
            self.remove_edge(parent_node, self.nodes[node_id])

        # Remove outgoing edges from the node
        for child_id in children:
            child_node = self.nodes[child_id]
            self.remove_edge(self.nodes[node_id], child_node)
        
        # Remove the node from the adjacency list and nodes dictionary
        del self.adjacency_list[node_id]
        del self.nodes[node_id]


    def get_neighbors(self, node: Node) -> List[Node]:
        """Get the outgoing neighbors of this `Node`"""
        return [self.nodes[id] for id in self.adjacency_list.get(node.id, [])]

    def get_predecessors(self, node: Node) -> List[Node]:
        """Get the predeccors of this node by following incoming edges"""
        return [self.nodes[id] for id, adj in self.adjacency_list.items() if node.id in adj]


    def to_dot(self) -> str:
        """Output the DataFlow graph in DOT (Graphviz) format."""
        lines = [f"digraph {self.operator_name}_{self.name} {{"]

        # Add nodes
        for node in self.nodes.values():
            lines.append(f'    {node.id} [label="{str(node)}"];')

        # Add edges
        for edge in self.edges:
            line = f"    {edge.from_node.id} -> {edge.to_node.id}"
            if edge.if_conditional is not None:
                line += f' [label="{edge.if_conditional}"]'
            line += ";"
            lines.append(line)

        lines.append("}")
        return "\n".join(lines)
    
    def generate_event(self, variable_map: dict[str, Any], key: Optional[str] = None) -> list['Event']:
            assert len(self.entry) != 0
            # give all the events the same id
            first_event = Event(self.entry[0], variable_map, self.ref(), key=key)
            id = first_event._id
            events = [first_event] + [Event(entry, variable_map, self.ref(), _id=id, key=key) for entry in self.entry[1:]] 
            
            # TODO: propogate at "compile time" instead of doing this every time
            local_events = []
            for ev in events:
                if isinstance(ev.target, CallRemote) or isinstance(ev.target, IfNode):
                    local_events.extend(ev.propogate(None, cascade.core.dataflows))
                else:
                    local_events.append(ev)

            return local_events

       
    def __str__(self) -> str:
        return f"{self.operator_name}.{self.name}" 

def metadata_dict() -> dict:
    return {
        "in_t": None,
        "deser_times": [],
        "flink_time": 0
    }

@dataclass
class CallStackItem:
    dataflow: DataflowRef
    assign_result_to: Optional[str]
    var_map: dict[str, str]
    """Variables are saved in the call stack"""
    targets: Union[Node, List[Node]]
    key: Optional[str] = None
    """The key to use when coming back"""

@dataclass
class Event():
    """An Event is an object that travels through the Dataflow graph."""
    
    target: 'Node'
    """The Node that this Event wants to go to."""

    variable_map: dict[str, Any]
    """A mapping of variable identifiers to values. 
    If `target` is an `OpNode` this map should include the variables needed for that method."""

    dataflow: DataflowRef
    """The Dataflow that this event is a part of. If None, it won't propogate.
    This might be remove in the future in favour of a routing operator."""

    _id: int = field(default=None) # type: ignore (will get updated in __post_init__ if unset)
    """Unique ID for this event. Except in `propogate`, this `id` should not be set."""
    

    call_stack: List[CallStackItem] = field(default_factory=list)
    """Target used when dataflow is done, used for recursive dataflows."""

    metadata: dict = field(default_factory=metadata_dict)
    """Event metadata containing, for example, timestamps for benchmarking"""

    key: Optional[str] = None
    """If on a Stateful Operator, the key of the state"""
    
    def __post_init__(self):
        if self._id is None:
            # Assign a unique ID 
            self._id = uuid.uuid4().int

    def propogate(self, result: Any, df_map: dict['DataflowRef','DataFlow']) -> Iterable[Union['EventResult', 'Event']]:
        """Propogate this event through the Dataflow."""
        targets = df_map[self.dataflow].get_neighbors(self.target)
        
        
        events = []

        if len(targets) == 0 and not isinstance(self.target, CallRemote):
            if len(self.call_stack) > 0:
                caller = self.call_stack.pop()

                new_df = caller.dataflow
                new_targets = caller.targets
                if not isinstance(new_targets, list):
                    new_targets = [new_targets]
                var_map = caller.var_map
                if (x := caller.assign_result_to):
                    var_map[x] = result

                for target in new_targets:
                    ev = Event(
                        target,
                        var_map, 
                        new_df,
                        _id=self._id,
                        call_stack=self.call_stack,
                        metadata=self.metadata,
                        key=caller.key
                    )
                    events.append(ev)            

            else:   
                yield EventResult(self._id, result, self.metadata)
                return
        else:
            current_node = self.target
            events = current_node.propogate(self, targets, result, df_map)

        for event in events:
            if isinstance(event.target, CallRemote) or isinstance(event.target, IfNode):
                # recursively propogate CallRemote events
                yield from event.propogate(None, df_map)
            else:
                yield event
@dataclass
class EventResult():
    event_id: int
    result: Any
    metadata: dict