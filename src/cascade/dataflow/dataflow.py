from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Type, Union
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Prevent circular imports
    from cascade.dataflow.operator import StatelessOperator
    

class Operator(ABC):
    pass

@dataclass
class InitClass:
    """A method type corresponding to an `__init__` call."""
    pass

@dataclass
class InvokeMethod:
    """A method invocation of the underlying method indentifier."""
    method_name: str

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
    def propogate_opnode(node: Union['OpNode', 'StatelessOpNode'], event: 'Event', targets: list[Node], result: Any) -> list['Event']:
        if event.collect_target is not None:
            # Assign new collect targets
            collect_targets = [
                event.collect_target for i in range(len(targets))
            ]
        else:
            # Keep old collect targets
            collect_targets = [node.collect_target for i in range(len(targets))]

        if node.is_conditional:
            edges = event.dataflow.nodes[event.target.id].outgoing_edges
            true_edges = [edge for edge in edges if edge.if_conditional]
            false_edges = [edge for edge in edges if not edge.if_conditional]
            if not (len(true_edges) == len(false_edges) == 1):
                print(edges)
            assert len(true_edges) == len(false_edges) == 1
            target_true = true_edges[0].to_node
            target_false = false_edges[0].to_node

            
            return [Event(
                target_true if result else target_false,
                event.variable_map,
                event.dataflow,
                _id=event._id,
                collect_target=ct)

                for ct in collect_targets]
        else:
            return [Event(
                    target,
                    event.variable_map, 
                    event.dataflow,
                    _id=event._id,
                    collect_target=ct)
                    
                    for target, ct in zip(targets, collect_targets)]

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
            collect_target=ct)
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
                    collect_target=ct)
                    
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
        merge{Merge}
        user1-- item1_key -->item1;
        user1-- item2_key -->item2;
        item1-- item1_price -->merge;
        item2-- item2_price -->merge;
        merge-- [item1_price, item2_price] -->user2;
    ```

    In code, one would write:

    ```py
    df = DataFlow("user.buy_items")
    n0 = OpNode(User, InvokeMethod("buy_items_0"))
    n1 = OpNode(Item, InvokeMethod("get_price"))
    n2 = OpNode(Item, InvokeMethod("get_price"))
    n3 = MergeNode()
    n4 = OpNode(User, InvokeMethod("buy_items_1"))
    df.add_edge(Edge(n0, n1))
    df.add_edge(Edge(n0, n2))
    df.add_edge(Edge(n1, n3))
    df.add_edge(Edge(n2, n3))
    df.add_edge(Edge(n3, n4))
    ```
    """
    def __init__(self, name: str):
        self.name: str = name
        self.adjacency_list: dict[int, list[int]] = {}
        self.nodes: dict[int, Node] = {}
        self.entry: Node = None

    def add_node(self, node: Node):
        """Add a node to the Dataflow graph if it doesn't already exist."""
        if node.id not in self.adjacency_list:
            self.adjacency_list[node.id] = []
            self.nodes[node.id] = node

    def add_edge(self, edge: Edge):
        """Add an edge to the Dataflow graph. Nodes that don't exist will be added to the graph automatically."""
        self.add_node(edge.from_node)
        self.add_node(edge.to_node)
        self.adjacency_list[edge.from_node.id].append(edge.to_node.id)
        edge.from_node.outgoing_edges.append(edge)

    def get_neighbors(self, node: Node) -> List[Node]:
        """Get the outgoing neighbors of this `Node`"""
        return [self.nodes[id] for id in self.adjacency_list.get(node.id, [])]

class Result(ABC):
    pass

@dataclass
class Arrived(Result):
    val: Any

@dataclass
class NotArrived(Result):
    pass

@dataclass
class CollectTarget:
    target_node: CollectNode
    """Target node"""
    total_items: int
    """How many items the merge node needs to wait on (including this one)."""
    result_idx: int
    """The index this result should be in the collected array."""

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

    _id_counter: int = field(init=False, default=0, repr=False)
    
    def __post_init__(self):
        if self._id is None:
            # Assign a unique ID from the class-level counter
            self._id = Event._id_counter
            Event._id_counter += 1

    def propogate(self, result, select_all_keys: Optional[list[str]]=None) -> Union['EventResult', list['Event']]:
        """Propogate this event through the Dataflow."""

        if self.dataflow is None:
            return EventResult(self._id, result)
        
        targets = self.dataflow.get_neighbors(self.target)
        
        if len(targets) == 0:
            return EventResult(self._id, result)
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