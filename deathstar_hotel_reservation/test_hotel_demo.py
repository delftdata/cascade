import logging
import sys
import os



sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from cascade.runtime.flink_runtime import FlinkClientSync
from cascade.dataflow.dataflow import DataflowRef
from cascade.dataflow.optimization.parallelization import parallelize, parallelize_until_if
from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from cascade.runtime.python_runtime import PythonClientSync, PythonRuntime

import cascade
import pytest
import tests.integration.flink.utils as utils

def init_python_runtime() -> tuple[PythonRuntime, PythonClientSync]:
    runtime = PythonRuntime()
    for op in cascade.core.operators.values():
        if isinstance(op, StatefulOperator):
            runtime.add_operator(op)
        elif isinstance(op, StatelessOperator):
            runtime.add_stateless_operator(op)
    
    runtime.run()
    return runtime, PythonClientSync(runtime)


def test_deathstar_hotel_demo_python():
    print("starting")
    cascade.core.clear() 
    exec(f'import deathstar_hotel_reservation.entities.user')
    cascade.core.init()

    runtime, client = init_python_runtime()
    deathstar_hotel_demo(client)

@pytest.mark.integration
def test_deathstar_hotel_demo_flink():
    print("starting")
    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")

    utils.create_topics()

    runtime = utils.init_flink_runtime("deathstar_hotel_reservation.entities.user")
    
    client = FlinkClientSync()
    runtime.run(run_async=True)

    try:
        deathstar_hotel_demo(client)
    finally:
        client.close()

def deathstar_hotel_demo(client):
    user_init = cascade.core.dataflows[DataflowRef("User", "__init__")]
    
    for df in cascade.core.dataflows.values():
        print(df.to_dot())

    print("testing user create")

    username = f"Delft_1"
    password = str(1) * 10
    event = user_init.generate_event({"username": username, "password": password}, key=username)
    result = client.send(event, block = True)
    assert result['username'] == username
    assert result['password'] == password

    print("testing user login")
