
import os
import sys


# import cascade
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

from cascade.dataflow.dataflow import DataflowRef
from cascade.dataflow.optimization.parallelization import parallelize_until_if
from cascade.runtime.python_runtime import PythonClientSync, PythonRuntime
import cascade

def test_parallelize():
    cascade.core.clear() # clear cascadeds registerd classes.
    assert not cascade.core.registered_classes, "Registered classes should be empty before importing a Cascade \
                                                    Module"
    # import the module
    import_module_name: str = 'entities'
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

    print(df.to_dot())
    df_parallel, _ = parallelize_until_if(df)
    df_parallel.name = "get_total_parallel"
    cascade.core.dataflows[DataflowRef("Test", "get_total_parallel")] = df_parallel

    print(df_parallel.to_dot())

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

def gen_parallel(df):
    par, rest = parallelize_until_if(df)

    # join the two dataflows
    par_exit = [node.id for node in par.nodes.values() if len(node.outgoing_edges) == 0]
    for edge in rest.edges:
        par.add_edge(edge)
    assert len(rest.entry) == 1
    assert len(par_exit) == 1
    par.add_edge_refs(par_exit[0], rest.entry[0].id, None)


    print(par.to_dot())
    par.name = df.name + "_parallel"
    return par

def test_code_motion():
    cascade.core.clear() # clear cascadeds registerd classes.
    assert not cascade.core.registered_classes, "Registered classes should be empty before importing a Cascade \
                                                    Module"
    # import the module
    import_module_name: str = 'code_motion_entities'
    exec(f'import tests.optimizations.{import_module_name}')
    
    cascade.core.init()

    print(cascade.core.operators)
    user_op = cascade.core.operators["User"]
    item_op = cascade.core.operators["Item"]
    item_init = cascade.core.dataflows[DataflowRef("Item", "__init__")]
    user_init = cascade.core.dataflows[DataflowRef("User", "__init__")]
    checkout = cascade.core.dataflows[DataflowRef("User", "checkout_item")]
    balance = cascade.core.dataflows[DataflowRef("User", "get_balance")]

    checkout_parallel = gen_parallel(checkout)
    print(checkout.to_dot())
    cascade.core.dataflows[DataflowRef("User", "checkout_item_parallel")] = checkout_parallel

    print(checkout_parallel.to_dot())

    assert len(checkout_parallel.entry) == 2
    assert len(checkout.entry) == 1

    runtime = PythonRuntime()
    runtime.add_operator(item_op)
    runtime.add_operator(user_op)
    runtime.run()

    client = PythonClientSync(runtime)

    event = item_init.generate_event({"item": "fork", "quantity": 10, "price": 10}, key="fork")
    result = client.send(event) 
    print(result)

    event = item_init.generate_event({"item": "spoon", "quantity": 0, "price": 10}, key="spoon")
    result = client.send(event) 
    print(result)

    event = item_init.generate_event({"item": "knife", "quantity": 10, "price": 100}, key="knife")
    result = client.send(event) 
    print(result)

    event = user_init.generate_event({"balance": 50}, key="user")
    result = client.send(event) 


    # buy spoon fails
    event = checkout.generate_event({"item_0": "spoon"}, key="user")
    result = client.send(event)
    assert not result

    event = checkout_parallel.generate_event({"item_0": "spoon"}, key="user")
    result = client.send(event)
    assert not result


    # buy knife fails
    event = checkout.generate_event({"item_0": "knife"}, key="user")
    result = client.send(event)
    assert not result

    event = checkout_parallel.generate_event({"item_0": "knife"}, key="user")
    result = client.send(event)
    assert not result


    # buy fork works!
    event = checkout.generate_event({"item_0": "fork"}, key="user")
    result = client.send(event)
    assert result

    event = checkout_parallel.generate_event({"item_0": "fork"}, key="user")
    result = client.send(event)
    assert result

    event = balance.generate_event({}, key="user")
    result = client.send(event)
    assert result == 30



