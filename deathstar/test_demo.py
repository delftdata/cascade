
import os
import sys

# import cascade
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from cascade.runtime.python_runtime import PythonClientSync, PythonRuntime
from cascade.runtime.flink_runtime import FlinkClientSync, FlinkRuntime
from deathstar.demo import DeathstarDemo, recommend, reserve, search_hotel, user_login
import time
import pytest

@pytest.mark.integration
def test_deathstar_demo():
    ds = DeathstarDemo()
    ds.init_runtime(FlinkRuntime("deathstardemo-test", "dsd-out"))
    ds.runtime.run(run_async=True)
    print("Populating, press enter to go to the next step when done")
    ds.populate()

    client = FlinkClientSync("deathstardemo-test", "dsd-out")
    input()
    print("testing user login")
    event = user_login()
    client.send(event)

    input()
    print("testing reserve")
    event = reserve()
    client.send(event)

    input()
    print("testing search")
    event = search_hotel()
    client.send(event)

    input()
    print("testing recommend (distance)")
    time.sleep(0.5)
    event = recommend(req_param="distance")
    client.send(event)

    input()
    print("testing recommend (price)")
    time.sleep(0.5)
    event = recommend(req_param="price")
    client.send(event)

    print(client._futures)
    input()
    print("done!")
    print(client._futures)

def test_deathstar_demo_python():
    ds = DeathstarDemo()
    ds.init_runtime(PythonRuntime())
    ds.runtime.run()
    print("Populating, press enter to go to the next step when done")
    ds.populate()

    time.sleep(2)

    client = PythonClientSync(ds.runtime)
    print("testing user login")
    event = user_login()
    result = client.send(event)
    assert result == True

    print("testing reserve")
    event = reserve()
    result = client.send(event)
    assert result == True

    print("testing search")
    event = search_hotel()
    result = client.send(event)
    print(result)

    print("testing recommend (distance)")
    time.sleep(0.5)
    event = recommend(req_param="distance")
    result = client.send(event)
    print(result)

    print("testing recommend (price)")
    time.sleep(0.5)
    event = recommend(req_param="price")
    result = client.send(event)
    print(result)

    print("done!")


if __name__ == "__main__":
    test_deathstar_demo()