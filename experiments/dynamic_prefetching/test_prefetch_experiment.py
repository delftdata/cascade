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


def test_prefetching_exp_test_python():
    print("starting")
    cascade.core.clear() 
    exec(f'import experiments.dynamic_prefetching.entities')
    cascade.core.init()

    
    baseline = cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_baseline")]
    prefetch = cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_prefetch")]


    pre_par = parallelize(prefetch)
    cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_prefetch_parallel")] = pre_par

    base_par = parallelize(baseline)
    cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_baseline_parallel")] = base_par


    runtime, client = init_python_runtime()
    prefetching_exp_test(client)

@pytest.mark.integration
def test_prefetching_exp_flink():
    print("starting")
    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")

    utils.create_topics()

    runtime = utils.init_flink_runtime("experiments.dynamic_prefetching.entities")
    
    baseline = cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_baseline")]
    prefetch = cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_prefetch")]


    pre_par = parallelize(prefetch)
    cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_prefetch_parallel")] = pre_par
    runtime.add_dataflow(pre_par)

    base_par = parallelize(baseline)
    cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_baseline_parallel")] = base_par
    runtime.add_dataflow(base_par)


    client = FlinkClientSync()
    runtime.run(run_async=True)

    try:
        prefetching_exp_test(client)
    finally:
        client.close()

def prefetching_exp_test(client):
    for df in cascade.core.dataflows.values():
        print(df.to_dot())

    username = "premium"

    print("testing user create")

    event = cascade.core.dataflows[DataflowRef("User", "__init__")].generate_event({"key": username, "is_premium": False, "preferences": ["museums"]}, username)
    result = client.send(event, block=True)
    print(result)
    assert result['key'] == username

    event = cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_baseline_parallel")].generate_event({"origin_0": 0, "dest_0": 0, "user_0": username})
    result = client.send(event, block=True)
    print(result)

    event = cascade.core.dataflows[DataflowRef("NavigationService", "get_directions_prefetch_parallel")].generate_event({"origin_0": 0, "dest_0": 0, "user_0": username})
    result = client.send(event, block=True)
    print(result)
