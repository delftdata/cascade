import os

import pytest 
import cascade
import sys


from cascade.dataflow.dataflow import Event
from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from cascade.runtime.python_runtime import PythonClientSync, PythonRuntime
from tests.programs.util import compare_targets_with_expected


target_program_relative_path: str = 'test_programs/target'
expected_program_relative_path: str = 'test_programs/expected'


def get_target_file_list():
    target_files: list[str] = os.listdir(target_program_relative_path)
    return list(filter(lambda f: f.endswith('.py') and '__init__' not in f, target_files))

target_files: list[str] = get_target_file_list()

# @pytest.mark.parametrize("file_name", target_files)
def test_checkout_item():
    file_name = "checkout_item.py"
    for key in list(sys.modules.keys()):
        if key.startswith("test_programs"):
            del sys.modules[key] 
        
    cascade.core.clear() # clear cascadeds registerd classes.
    assert not cascade.core.registered_classes, "Registered classes should be empty before importing a Cascade \
                                                    Module"
    # import the module
    import_module_name: str = f'test_programs.target.{file_name.strip(".py")}'
    exec(f'import {import_module_name}')
    
    cascade.core.init()
    assert cascade.core.registered_classes, "The Cascade module classes should be registered at this point."

    for op in cascade.core.operators.values():
        print(op.methods)

    runtime, client = init_python_runtime()
    item_op = cascade.core.operators["Item"]
    user_op = cascade.core.operators["User"]
    event = item_op.dataflows["__init__"].generate_event({"item_name": "fork", "price": 10}, key="fork")
    result = client.send(event)
    assert result.price == 10
    assert result.item_name == "fork"

    event = item_op.dataflows["__init__"].generate_event({"item_name": "spoon", "price": 20}, key="spoon")
    result = client.send(event)
    assert result.price == 20
    assert result.__key__() == "spoon"

    event = user_op.dataflows["__init__"].generate_event({"username": "test", "balance": 15}, key="test")
    user = client.send(event)
    assert user.balance == 15
    assert user.__key__() == "test"

    event = user_op.dataflows["buy_item"].generate_event({"item_0": "fork"}, key=user.__key__())
    result = client.send(event)
    assert runtime.statefuloperators["User"].states["test"]["balance"] == 5
    assert result 

    event = user_op.dataflows["buy_item"].generate_event({"item_0": "spoon"}, key=user.__key__())
    result = client.send(event)
    assert runtime.statefuloperators["User"].states["test"]["balance"] == -15
    assert not result 

    
def test_operator_chaining():
    file_name = "operator_chaining.py"
    for key in list(sys.modules.keys()):
        if key.startswith("test_programs"):
            del sys.modules[key] 
        
    cascade.core.clear() 
    import_module_name: str = f'test_programs.target.{file_name.strip(".py")}'
    exec(f'import {import_module_name}')
    cascade.core.init()

    for op in cascade.core.operators.values():
        print(op.methods)

    for df in cascade.core.dataflows.values():
        print(df.to_dot())

    runtime, client = init_python_runtime()
    a_op = cascade.core.operators["A"]
    b_op = cascade.core.operators["B"]
    c_op = cascade.core.operators["C"]
    
    event = a_op.dataflows["__init__"].generate_event({"key": "aaa"}, key="aaa")
    result = client.send(event)
    assert result.key == "aaa"

    event = b_op.dataflows["__init__"].generate_event({"key": "bbb"}, key="bbb")
    result = client.send(event)
    assert result.key == "bbb"

    event = c_op.dataflows["__init__"].generate_event({"key": "ccc"}, key="ccc")
    result = client.send(event)
    assert result.key == "ccc"

    event = c_op.dataflows["get"].generate_event({"y_0": 0}, key="ccc")
    result = client.send(event)
    assert result == 42

    print("Call C")
    event = b_op.dataflows["call_c"].generate_event({ "c_0": "ccc"}, key="bbb")
    print(event)
    result = client.send(event)
    assert result == 42

    print("call C thru B")
    event = a_op.dataflows["call_c_thru_b"].generate_event({"b_0": "bbb", "c_0": "ccc"}, key="aaa")
    result = client.send(event)
    assert result == 84

def init_python_runtime() -> tuple[PythonRuntime, PythonClientSync]:
    runtime = PythonRuntime()
    for op in cascade.core.operators.values():
        if isinstance(op, StatefulOperator):
            runtime.add_operator(op)
        elif isinstance(op, StatelessOperator):
            runtime.add_stateless_operator(op)
    
    runtime.run()
    return runtime, PythonClientSync(runtime)
