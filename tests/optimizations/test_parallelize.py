
import os
import sys


# import cascade
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

from cascade.dataflow.dataflow import DataflowRef
from cascade.dataflow.optimization.parallelization import parallelize
from cascade.runtime.python_runtime import PythonClientSync, PythonRuntime
import cascade

def test_parallelize():
    cascade.core.clear() # clear cascadeds registerd classes.
    assert not cascade.core.registered_classes, "Registered classes should be empty before importing a Cascade \
                                                    Module"
    # import the module
    import_module_name: str = 'test_ops'
    exec(f'import tests.optimizations.{import_module_name}')
    
    cascade.core.init()

    print(cascade.core.operators)
    test_op = cascade.core.operators["Test"]
    adder_op = cascade.core.operators["Adder"]
    stock_op = cascade.core.operators["Stock"]
    stock_init = cascade.core.dataflows[DataflowRef("Stock", "__init__")]
    df = cascade.core.dataflows[DataflowRef("Test", "get_total")]
    print(df)
    print(df.nodes)

    df_parallel = parallelize(df)
    df_parallel.name = "get_total_parallel"
    cascade.core.dataflows[DataflowRef("Test", "get_total_parallel")] = df_parallel

    assert len(df_parallel.entry) == 2
    assert len(df.entry) == 1

    runtime = PythonRuntime()
    runtime.add_stateless_operator(test_op)
    runtime.add_stateless_operator(adder_op)
    runtime.add_operator(stock_op)
    runtime.run()

    client = PythonClientSync(runtime)

    event = stock_init.generate_event({"item": "fork", "quantity": 10}, key="fork")
    result = client.send(event) 
    

    event = stock_init.generate_event({"item": "spoon", "quantity": 20}, key="spoon")
    result = client.send(event) 

    event = df.generate_event({"item1_0": "fork", "item2_0": "spoon"})
    result = client.send(event)
    assert result == 30

    event = df_parallel.generate_event({"item1_0": "fork", "item2_0": "spoon"})
    result = client.send(event)
    assert result == 30
