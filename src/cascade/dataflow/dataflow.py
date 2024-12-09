from abc import ABC
from dataclasses import dataclass, field
from typing import Any, List, Optional, Union


@dataclass
class InitClass:
    """A method type corresponding to an `__init__` call."""
    pass

@dataclass
class InvokeMethod:
    """A method invocation of the underlying method indentifier."""
    method_name: str

@dataclass
class Node(ABC):
    """Base class for Nodes."""
    id: int = field(init=False)
    """This node's unique id."""

    _id_counter: int = field(init=False, default=0, repr=False)
    
    def __post_init__(self):
        # Assign a unique ID from the class-level counter
        self.id = Node._id_counter
        Node._id_counter += 1

@dataclass
class OpNode(Node):
    """A node in a `Dataflow` corresponding to a method call of a `StatefulOperator`. 
    
    A `Dataflow` may reference the same `StatefulOperator` multiple times. 
    The `StatefulOperator` that this node belongs to is referenced by `cls`."""
    cls: Any
    method_type: Union[InitClass, InvokeMethod]

@dataclass
class MergeNode(Node):
    """A node in a `Dataflow` corresponding to a merge operator. 
    
    It will aggregate incoming edges and output them as a list to the outgoing edge.
    Their actual implementation is runtime-dependent."""
    pass

@dataclass
class Edge():
    """An Edge in the Dataflow graph."""
    from_node: Node
    to_node: Node

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
    def __init__(self, name):
        self.name = name
        self.adjacency_list = {}
        self.nodes = {}

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

    def get_neighbors(self, node: Node) -> List[Node]:
        """Get the outgoing neighbors of this `Node`"""
        return [self.nodes[id] for id in self.adjacency_list.get(node.id, [])]

@dataclass
class Event():
    """An Event is an object that travels through the Dataflow graph."""
    
    target: 'Node'
    """The Node that this Event wants to go to."""

    key_stack: list[str]
    """The keys this event is concerned with. 
    The top of the stack, i.e. `key_stack[-1]`, should always correspond to a key 
    on the StatefulOperator of `target.cls` if `target` is an `OpNode`."""

    args: List[Any]
    kwargs: dict[str, Any]
    """The args and kwargs to be passed to the `target`. 
    If `target` is an `OpNode` this corresponds to the method args/kwargs."""

    dataflow: Optional['DataFlow']
    """The Dataflow that this event is a part of. If None, it won't propogate.
    This might be remove in the future in favour of a routing operator."""

    _id: int = field(default=None)
    """Unique ID for this event. Except in `propogate`, this `id` should not be set."""
    _id_counter: int = field(init=False, default=0, repr=False)
    
    def __post_init__(self):
        if self._id is None:
            # Assign a unique ID from the class-level counter
            self._id = Event._id_counter
            Event._id_counter += 1

    def propogate(self, key_stack, args, kwargs) -> list['Event']:
        """Propogate this event through the Dataflow."""
        if self.dataflow is None or len(key_stack) == 0:
            self.args = args
            self.kwargs = kwargs
            return [self]
        
        targets = self.dataflow.get_neighbors(self.target)
        
        if len(targets) == 0:
            self.args = args
            self.kwargs = kwargs
            return [self]
        else:
            # An event with multiple targets should have the same number of keys in a list on top of its key stack
            keys = key_stack.pop()
            if not isinstance(keys, list):
                keys = [keys]
            return [Event(
                target,
                key_stack + [key],
                args,
                kwargs, 
                self.dataflow,
                _id=self._id)
                
                for target, key in zip(targets, keys)]