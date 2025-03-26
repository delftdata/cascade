from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Type, Union
from typing import TYPE_CHECKING
import uuid

if TYPE_CHECKING:
    # Prevent circular imports
    from cascade.dataflow.operator import StatelessOperator
    

class Operator(ABC):
    @abstractmethod
    def name(self) -> str:
        pass

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
class Filter:
    """Filter by this function"""
    filter_fn: Callable

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
    def propogate(self, event: 'Event', targets: list['Node'], result: Any, **kwargs) -> list['Event']:
        pass

@dataclass
class OpNode(Node):
    """A node in a `Dataflow` corresponding to a method call of a `StatefulOperator`. 
    
    A `Dataflow` may reference the same entity multiple times. 
    The `StatefulOperator` that this node belongs to is referenced by `entity`."""
    entity: Type
    method_type: Union[InitClass, InvokeMethod, Filter]
    read_key_from: str
    """Which variable to take as the key for this StatefulOperator"""

    assign_result_to: Optional[str] = field(default=None)
    """What variable to assign the result of this node to, if any."""
    is_conditional: bool = field(default=False)
    """Whether or not the boolean result of this node dictates the following path."""
    collect_target: Optional['CollectTarget'] = field(default=None)
    """Whether the result of this node should go to a CollectNode."""

    def propogate(self, event: 'Event', targets: List[Node], result: Any) -> list['Event']:
        return OpNode.propogate_opnode(self, event, targets, result)

    @staticmethod
    def propogate_opnode(node: Union['OpNode', 'StatelessOpNode'], event: 'Event', targets: list[Node], 
                         result: Any) -> list['Event']:
        num_targets = 1 if node.is_conditional else len(targets)

        if event.collect_target is not None:
            # Assign new collect targets
            collect_targets = [
                event.collect_target for i in range(num_targets)
            ]
        else:
            # Keep old collect targets
            collect_targets = [node.collect_target for i in range(num_targets)]

        if node.is_conditional:
            edges = event.dataflow.nodes[event.target.id].outgoing_edges
            true_edges = [edge for edge in edges if edge.if_conditional]
            false_edges = [edge for edge in edges if not edge.if_conditional]
            if not (len(true_edges) == len(false_edges) == 1):
                print(edges)
            assert len(true_edges) == len(false_edges) == 1
            target_true = true_edges[0].to_node
            target_false = false_edges[0].to_node

            assert len(collect_targets) == 1, "num targets should be 1"
            ct = collect_targets[0]

            return [Event(
                target_true if result else target_false,
                event.variable_map,
                event.dataflow,
                _id=event._id,
                collect_target=ct,
                metadata=event.metadata)
            ]

        else:
            return [Event(
                    target,
                    event.variable_map, 
                    event.dataflow,
                    _id=event._id,
                    collect_target=ct,
                    metadata=event.metadata)
                    
                    for target, ct in zip(targets, collect_targets)]
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.entity.__name__}, {self.method_type})"

@dataclass
class StatelessOpNode(Node):
    """A node in a `Dataflow` corresponding to a method call of a `StatelessOperator`. 
    
    A `Dataflow` may reference the same `StatefulOperator` multiple times. 
    The `StatefulOperator` that this node belongs to is referenced by `cls`."""
    operator: 'StatelessOperator'
    method_type: InvokeMethod
    """Which variable to take as the key for this StatefulOperator"""
    
    assign_result_to: Optional[str] = None
    """What variable to assign the result of this node to, if any."""
    is_conditional: bool = False
    """Whether or not the boolean result of this node dictates the following path."""
    collect_target: Optional['CollectTarget'] = None
    """Whether the result of this node should go to a CollectNode."""

    def propogate(self, event: 'Event', targets: List[Node], result: Any) -> List['Event']:
        return OpNode.propogate_opnode(self, event, targets, result)
    
@dataclass
class DataflowNode(Node):
    """A node in a `DataFlow` corresponding to the call of another dataflow"""
    dataflow: 'DataFlow'
    variable_rename: dict[str, str]
    
    assign_result_to: Optional[str] = None
    """What variable to assign the result of this node to, if any."""
    is_conditional: bool = False
    """Whether or not the boolean result of this node dictates the following path."""
    collect_target: Optional['CollectTarget'] = None
    """Whether the result of this node should go to a CollectNode."""

    def propogate(self, event: 'Event', targets: List[Node], result: Any) -> List['Event']:
        # remap the variable map of event into the new event

        # add the targets as some sort of dataflow "exit nodes"
        return self.dataflow


@dataclass
class SelectAllNode(Node):
    """A node type that will yield all items of an entity filtered by 
    some function.
    
    Think of this as executing `SELECT * FROM cls`"""
    cls: Type
    collect_target: 'CollectNode'
    assign_key_to: str

    def propogate(self, event: 'Event', targets: List[Node], result: Any, keys: list[str]):
        targets = event.dataflow.get_neighbors(event.target)
        assert len(targets) == 1
        n = len(keys)
        collect_targets = [
            CollectTarget(self.collect_target, n, i)
            for i in range(n)
        ]
        return [Event(
            targets[0],
            event.variable_map | {self.assign_key_to: key}, 
            event.dataflow,
            _id=event._id,
            collect_target=ct,
            metadata=event.metadata)
            for ct, key in zip(collect_targets, keys)]

@dataclass
class CollectNode(Node):
    """A node in a `Dataflow` corresponding to a merge operator. 
    
    It will aggregate incoming edges and output them as a list to the outgoing edge.
    Their actual implementation is runtime-dependent."""
    assign_result_to: str
    """The variable name in the variable map that will contain the collected result."""
    read_results_from: str
    """The variable name in the variable map that the individual items put their result in."""

    def propogate(self, event: 'Event', targets: List[Node], result: Any, **kwargs) -> List['Event']:
        collect_targets = [event.collect_target for i in range(len(targets))]
        return [Event(
                    target,
                    event.variable_map, 
                    event.dataflow,
                    _id=event._id,
                    collect_target=ct,
                    metadata=event.metadata)
                    
                    for target, ct in zip(targets, collect_targets)]

@dataclass
class Edge():
    """An Edge in the Dataflow graph."""
    from_node: Node
    to_node: Node
    variable_map: dict[str, Any] = field(default_factory=dict)
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
    def __init__(self, name: str):
        self.name: str = name
        self.adjacency_list: dict[int, list[int]] = {}
        self.nodes: dict[int, Node] = {}
        self.entry: Union[Node, List[Node]] = None

    def add_node(self, node: Node):
        """Add a node to the Dataflow graph if it doesn't already exist."""
        if node.id not in self.adjacency_list:
            self.adjacency_list[node.id] = []
            self.nodes[node.id] = node

    def add_edge(self, edge: Edge):
        """Add an edge to the Dataflow graph. Nodes that don't exist will be added to the graph automatically."""
        self.add_node(edge.from_node)
        self.add_node(edge.to_node)
        if edge.to_node.id not in self.adjacency_list[edge.from_node.id]:
            self.adjacency_list[edge.from_node.id].append(edge.to_node.id)
            edge.from_node.outgoing_edges.append(edge)

    
    def remove_edge(self, from_node: Node, to_node: Node):
        """Remove an edge from the Dataflow graph."""
        if from_node.id in self.adjacency_list and to_node.id in self.adjacency_list[from_node.id]:
            # Remove from adjacency list
            self.adjacency_list[from_node.id].remove(to_node.id)
            # Remove from outgoing_edges
            from_node.outgoing_edges = [
                edge for edge in from_node.outgoing_edges if edge.to_node.id != to_node.id
            ]

    def remove_node(self, node: Node):
        """Remove a node from the DataFlow graph and reconnect its parents to its children."""
        if node.id not in self.nodes:
            return  # Node doesn't exist in the graph


        if isinstance(node, OpNode) or isinstance(node, StatelessOpNode):
            assert not node.is_conditional, "there's no clear way to remove a conditional node"
            assert not node.assign_result_to, "can't delete node whose result is used"
            assert not node.collect_target, "can't delete node which has a collect_target"

        # Find parents (nodes that have edges pointing to this node)
        parents = [parent_id for parent_id, children in self.adjacency_list.items() if node.id in children]

        # Find children (nodes that this node points to)
        children = self.adjacency_list[node.id]
        
        # Set df entry
        if self.entry == node:
            print(children)
            assert len(children) == 1, "cannot remove entry node if it doesn't exactly one child"
            self.entry = self.nodes[children[0]]

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
            self.remove_edge(parent_node, node)

        # Remove outgoing edges from the node
        for child_id in children:
            child_node = self.nodes[child_id]
            self.remove_edge(node, child_node)
        
        

        # Remove the node from the adjacency list and nodes dictionary
        del self.adjacency_list[node.id]
        del self.nodes[node.id]


    def get_neighbors(self, node: Node) -> List[Node]:
        """Get the outgoing neighbors of this `Node`"""
        return [self.nodes[id] for id in self.adjacency_list.get(node.id, [])]

    def to_dot(self) -> str:
        """Output the DataFlow graph in DOT (Graphviz) format."""
        lines = [f"digraph {self.name} {{"]

        # Add nodes
        for node in self.nodes.values():
            lines.append(f'    {node.id} [label="{node}"];')

        # Add edges
        for from_id, to_ids in self.adjacency_list.items():
            for to_id in to_ids:
                lines.append(f"    {from_id} -> {to_id};")

        lines.append("}")
        return "\n".join(lines)
    
    def generate_event(self, variable_map: dict[str, Any]) -> Union['Event', list['Event']]:
        if isinstance(self.entry, list):
            assert len(self.entry) != 0
            # give all the events the same id
            first_event = Event(self.entry[0], variable_map, self)
            id = first_event._id
            return [first_event] + [Event(entry, variable_map, self, _id=id) for entry in self.entry[1:]] 
        else:
            return Event(self.entry, variable_map, self)
    
@dataclass
class CollectTarget:
    target_node: CollectNode
    """Target node"""
    total_items: int
    """How many items the merge node needs to wait on (including this one)."""
    result_idx: int
    """The index this result should be in the collected array."""

def metadata_dict() -> dict:
    return {
        "in_t": None,
        "deser_times": [],
        "flink_time": 0
    }

@dataclass
class Event():
    """An Event is an object that travels through the Dataflow graph."""
    
    target: 'Node'
    """The Node that this Event wants to go to."""

    variable_map: dict[str, Any]
    """A mapping of variable identifiers to values. 
    If `target` is an `OpNode` this map should include the variables needed for that method."""

    dataflow: Optional['DataFlow']
    """The Dataflow that this event is a part of. If None, it won't propogate.
    This might be remove in the future in favour of a routing operator."""

    _id: int = field(default=None) # type: ignore (will get updated in __post_init__ if unset)
    """Unique ID for this event. Except in `propogate`, this `id` should not be set."""
    
    collect_target: Optional[CollectTarget] = field(default=None)
    """Tells each mergenode (key) how many events to merge on"""

    # _id_counter: int = field(init=False, default=0, repr=False)

    metadata: dict = field(default_factory=metadata_dict)
    """Event metadata containing, for example, timestamps for benchmarking"""
    
    def __post_init__(self):
        if self._id is None:
            # Assign a unique ID from the class-level counter
            self._id = uuid.uuid4().int
            # self._id = Event._id_counter
            # Event._id_counter += 1

    def propogate(self, result, select_all_keys: Optional[list[str]]=None) -> Union['EventResult', list['Event']]:
        """Propogate this event through the Dataflow."""

        if self.dataflow is None:
            return EventResult(self._id, result, self.metadata)
        
        targets = self.dataflow.get_neighbors(self.target)
        
        if len(targets) == 0:
            return EventResult(self._id, result, self.metadata)
        else:
            current_node = self.target

            if isinstance(current_node, SelectAllNode):
                assert select_all_keys
                return current_node.propogate(self, targets, result, select_all_keys)
            else:
                return current_node.propogate(self, targets, result)

@dataclass
class EventResult():
    event_id: int
    result: Any
    metadata: dict