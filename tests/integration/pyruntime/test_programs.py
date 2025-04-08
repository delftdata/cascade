
import cascade
import sys

from cascade.dataflow.dataflow import DataflowRef
from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from cascade.runtime.python_runtime import PythonClientSync, PythonRuntime
from tests.integration.pyruntime.utils import init_python_runtime


def test_checkout_item():
    file_name = "tests.integration.pyruntime.checkout_item"

    runtime, client = init_python_runtime(file_name)
    item_op = cascade.core.operators["Item"]
    user_op = cascade.core.operators["User"]
    user_init = cascade.core.dataflows[DataflowRef("User", "__init__")]
    user_buy_item = cascade.core.dataflows[DataflowRef("User", "buy_item")]
    item_init = cascade.core.dataflows[DataflowRef("Item", "__init__")]

    event = item_init.generate_event({"item_name": "fork", "price": 10}, key="fork")
    result = client.send(event)
    assert result.price == 10
    assert result.item_name == "fork"

    event = item_init.generate_event({"item_name": "spoon", "price": 20}, key="spoon")
    result = client.send(event)
    assert result.price == 20
    assert result.__key__() == "spoon"

    event = user_init.generate_event({"username": "test", "balance": 15}, key="test")
    user = client.send(event)
    assert user.balance == 15
    assert user.__key__() == "test"

    event = user_buy_item.generate_event({"item_0": "fork"}, key=user.__key__())
    result = client.send(event)
    assert runtime.statefuloperators["User"].states["test"]["balance"] == 5
    assert result 

    event = user_buy_item.generate_event({"item_0": "spoon"}, key=user.__key__())
    result = client.send(event)
    assert runtime.statefuloperators["User"].states["test"]["balance"] == -15
    assert not result 
    
def test_operator_chaining():
    file_name = "tests.integration.pyruntime.operator_chaining"

    runtime, client = init_python_runtime(file_name)
    a_op = cascade.core.operators["A"]
    b_op = cascade.core.operators["B"]
    c_op = cascade.core.operators["C"]
    a_init = cascade.core.dataflows[DataflowRef("A", "__init__")]
    b_init = cascade.core.dataflows[DataflowRef("B", "__init__")]
    c_init = cascade.core.dataflows[DataflowRef("C", "__init__")]
    c_get = cascade.core.dataflows[DataflowRef("C", "get")]
    b_call_c = cascade.core.dataflows[DataflowRef("B", "call_c")]
    a_call_c = cascade.core.dataflows[DataflowRef("A", "call_c_thru_b")]
    
    event = a_init.generate_event({"key": "aaa"}, key="aaa")
    result = client.send(event)
    assert result.key == "aaa"

    event = b_init.generate_event({"key": "bbb"}, key="bbb")
    result = client.send(event)
    assert result.key == "bbb"

    event = c_init.generate_event({"key": "ccc"}, key="ccc")
    result = client.send(event)
    assert result.key == "ccc"

    event = c_get.generate_event({"y_0": 0}, key="ccc")
    result = client.send(event)
    assert result == 42

    print("Call C")
    event = b_call_c.generate_event({ "c_0": "ccc"}, key="bbb")
    print(event)
    result = client.send(event)
    assert result == 42

    print("call C thru B")
    event = a_call_c.generate_event({"b_0": "bbb", "c_0": "ccc"}, key="aaa")
    result = client.send(event)
    assert result == 84


def test_branching_integration():
    file_name = "tests.integration.branching"

    runtime, client = init_python_runtime(file_name)
    branch = cascade.core.dataflows[DataflowRef("Brancher", "branch")]
    print(branch.to_dot())

    event = branch.generate_event({"cond_0": True})
    result = client.send(event)
    assert result == 33

    event = branch.generate_event({"cond_0": False})
    result = client.send(event)
    assert result == 42