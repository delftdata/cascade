"""
When is it safe to parallize nodes?

-> When they don't affect each other
-> The simpelest way of doing it could be to run individual dataflows in parallel
(e.g. item.get_price() can run in parallel)
-> must convey that we assume no side-affects, so the actual order of execution 
does not matter. could go deeper and give a spec.
-> some instructions from the same dataflow could also be completed in parallel?
maybe? like ILP. but might need to think of more contrived examples/do more
advanced program analyis.

From Control Flow to Dataflow
3. Parallelizing Memory Operations
- operations on different memory locatiosn need not be sequentialized
- circulate a set of access tokens for each variable (=split function?)
    - assume that every variable denotes a unique memory location (no aliasing)

We have to be careful about certain types of parallelization. Consider the example:

```
# Calculate the average item price in basket: List[Item]
n = 0
p = 0
for item in basket:
    n += 1
    p += item.price()
return p / n
```

In this example we would want to parallelize the calls to item.price().
But we have to make sure the calls to `n += 1` remains bounded to the number of 
items, even though there is no explicit data dependency.


----


There is another type of optimization we could look at.
Suppose the following:

```
n = self.basket_size

prices = [item.price() for item in self.basket]
total_price = sum(prices)

return total_price / n
```

In this case, the variable n is not needed in the list comprehension - unoptimized
versions would generate an extra function instead of having the line be re-ordered
into the bottom function. Instead, analyis of the variables each function needs
access to would be a way to optimize these parts!

--> Ask Soham about this!

from "From control flow to dataflow"

Consider the portion of control-flow graph between a node N and its *immediate 
postdominator* P. Every control-flow path starting at N ultimately ends up at P.
Suppose that there is no reference to a variable x in any node on any path between
N and P. It is clear that an access token for x that enters N may bypass this
region of the graph altogether and go directly to P.


----

"Dataflow-Based Parallelization of Control-Flow Algorithms"

loop invariant hoisting

```
i = 0
while i < n:
    x = y + z
    a[i] = 6 * i + x * x
    i += 1
```

can be transformed in

```
i = 0
if i < n:
    x = y + z                   # loop invariant 1
    t1 = x * x                  # loop invariant 2
    do {                        # do while loop needed in case the conditional has side effects
        a[i] = 6 * i + t1
        i += 1
    } while i < n
```

this is achieved using reaching definitions analysis. In the paper:
"It is a common optimization to pull those parts of a loop body
that depend on only static datasets outside of the loop, and thus
execute these parts only once [7 , 13 , 15 , 32 ]. However, launching
new dataflow jobs for every iteration step prevents this optimiza-
tion in the case of such binary operators where only one input is
static. For example, if a static dataset is used as the build-side of
a hash join, then the system should not rebuild the hash table at
every iteration step. Labyrinth operators can keep such a hash
table in their internal states between iteration steps. This is made
possible by implementing iterations as a single cyclic dataflow
job, where the lifetimes of operators span all the steps."
Is there a similair example we could leverage for cascade? one with a "static dataset" as loop invariant?
in spark, it's up to the programmer to .cache it


In this paper, they also use an intermediate representation of one "basic block" per node.
A "basic block" is a sequence of instructions that always execute one after the other,
in other words contains no control flow. Control flow is defined by the edges in the 
dataflow graph that connect the nodes.

There's also a slightly different focus of this paper. The focus is not on stateful
dataflows, and obviously the application is still focused on bigdata-like applications,
not ones were latency is key issue.


Basic Blocks - Aho, A. V., Sethi, R., and Ullman, J. D. Compilers: principles, techniques, and
tools, vol. 2. Addison-wesley Reading, 2007.
SSA - Rastello, F. SSA-based Compiler Design. Springer Publishing Company,
Incorporated, 2016.


----

ideas from "optimization of dataflows with UDFs:"

we are basically making a DSL (integrated with python) which would allow for optimization
of UDFs!! this optimization is inside the intermediate representation, and not directly in
the target machine (similair to Emma, which uses a functional style *but* is a DSL (does it
allow for arbitrary scala code?)) 

---

our program is essentially a compiler. this allows to take inspiration from existing
works on compilation (which has existed for much longer than work on dataflows (?) - 
actually, dataflows were more popular initially when people didn't settle on the von Neumann architecture yet,
see e.g. Monsoon (1990s) or the original control flow to dataflow paper. the popularisation and efficiency of tools
such as drayadlinq, apache spark, apache flink has reinvigorated the attention towards dataflows). 
BUT compilers are often have hardware specific optimizations, based on the hardware instruction sets, or hardware-specifics
such as optimization of register allocation, cache line considerations etc etc.
The compiler in Cascade/other cf to df systems do not necessarily have the same considerations. This is because the backend
is software rather than hardware (e.g. we use flink + kafka). Since software is generally a lot more flexible than hardware,
we can instead impose certain considerations on the execution engine (which is now software, instead of a chip) rather than
the other way around (e.g. SIMD introduced --> compiler optimizations introduced). (to be fair, compiler design has had major influences [citation needed] on CPU design, but the point is that hardware iteration
is generally slower and more expensive than software iteration).


---

for certain optimizations, cascade assumes order of any side effects (such as file IO) does not matter.
otherwise a lot of parallelization operations would become much more costly due to the necessary synchronization issues.

---

other optimization: code duplication

this would remove nodes (assumption that less nodes = faster) at the cost of more computation per node.
a common example is something like this:

```
cost = item.price()
if cost > 30:
    shipping_discount = discount_service.get_shipping_discount()
    price = cost * shipping_discount  
else:
    price = cost

return price
```

in this case the "return price" could be duplicated accross the two branches,
such that they don't need to return back to the function body.

---

other ideas: 
    https://en.wikipedia.org/wiki/Optimizing_compiler#Specific_techniques
"""

from dataclasses import dataclass
from typing import Any
from cascade.dataflow.dataflow import CallEntity, CallLocal, CollectNode, DataFlow, Edge, Node


@dataclass
class AnnotatedNode:
    node: Node
    reads: list[str]
    writes: list[str]
    

import networkx as nx
def parallelize(df: DataFlow):    
    # create the dependency graph
    ans = []
    # since we use SSA, every variable has exactly one node that writes it
    write_nodes = {} 
    graph = nx.DiGraph()
    for node in df.nodes.values():
        if isinstance(node, CallEntity):
            reads = list(node.variable_rename.values())
            writes = [result] if (result := node.assign_result_to) else []
        elif isinstance(node, CallLocal):
            method = df.get_operator().methods[node.method.method_name]
            reads = method.var_map_reads
            writes = method.var_map_writes
        else:
            raise ValueError(f"unsupported node type: {type(node)}")
        
        write_nodes.update({var: node.id for var in writes})

        ans.append(AnnotatedNode(node, reads, writes))
        graph.add_node(node.id)

    nodes_with_indegree_0 = set(graph.nodes)
    n_map = df.nodes
    for node in ans:
        for read in node.reads:
            if read in write_nodes:
                # "read" will not be in write nodes if it is part of the arguments
                # a more thorough implementation would not need the if check,
                # and add the arguments as writes to some function entry node
                graph.add_edge(write_nodes[read], node.node.id)
                try:
                    nodes_with_indegree_0.remove(node.node.id)
                except KeyError:
                    pass

    updated = DataFlow(df.name, df.op_name)
    updated.entry = [n_map[node_id] for node_id in nodes_with_indegree_0]
    prev_node = None

    while len(nodes_with_indegree_0) > 0:
        # remove nodes from graph
        children = []
        for node_id in nodes_with_indegree_0:
            children.extend(graph.successors(node_id))
            graph.remove_node(node_id)
            updated.add_node(n_map[node_id])
            

        # check for new indegree 0 nodes
        next_nodes = set()
        for child in children:
            if graph.in_degree(child) == 0:
                next_nodes.add(child)
        
        if len(nodes_with_indegree_0) > 1:
            # TODO: maybe collect node should just infer from it's predecessors?
            # like it can only have DataFlowNode predecessors
            # TODO: rename DataflowNode to EntityCall
            collect_node = CollectNode()
            for node_id in nodes_with_indegree_0:
                if prev_node:
                    updated.add_edge(Edge(prev_node, n_map[node_id]))
                updated.add_edge(Edge(n_map[node_id], collect_node))
            prev_node = collect_node
        else:
            node_id = nodes_with_indegree_0.pop()
            if prev_node:
                updated.add_edge(Edge(prev_node, n_map[node_id]))

            prev_node = n_map[node_id]

        nodes_with_indegree_0 = next_nodes

    return updated
