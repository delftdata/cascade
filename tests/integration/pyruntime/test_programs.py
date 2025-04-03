
import cascade
import sys

from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from cascade.runtime.python_runtime import PythonClientSync, PythonRuntime
from tests.integration.pyruntime.utils import init_python_runtime


def test_checkout_item():
    file_name = "checkout_item.py"

    runtime, client = init_python_runtime(file_name)
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

    runtime, client = init_python_runtime(file_name)
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
