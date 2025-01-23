from cascade.dataflow.dataflow import DataFlow, InvokeMethod
from cascade.dataflow.operator import StatefulOperator, StatelessOperator
import inspect

def is_no_op(func):
    # Get the source code of the function
    source = inspect.getsource(func).strip()
    
    # Extract the function body (skip the signature)
    lines = source.splitlines()
    if len(lines) < 2:
        # A function with only a signature can't have a body
        return False

    # Check the body of the function
    body = lines[1].strip()
    # A valid no-op function body is either 'pass' or 'return'
    return body in ("pass", "return")


def dead_node_elimination(stateful_ops: list[StatefulOperator], stateless_ops: list[StatelessOperator]):
    # Find dead functions
    dead_func_names = set()
    for op in stateful_ops:
        for method in op._methods.values():
            if is_no_op(method):
                dead_func_names.add(method.__qualname__)
        
    # Remove them from dataflows
    for op in stateful_ops:
        for dataflow in op.dataflows.values():
            to_remove = []
            for node in dataflow.nodes.values():
                if hasattr(node, "method_type") and isinstance(node.method_type, InvokeMethod):
                    im: InvokeMethod = node.method_type
                    if im.method_name in dead_func_names:
                        to_remove.append(node)
            
            for node in to_remove:
                print(node)
                dataflow.remove_node(node)
                print(dataflow.to_dot())


