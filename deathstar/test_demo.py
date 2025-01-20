
from deathstar.demo import DeathstarDemo, DeathstarClient
import time
import pytest

@pytest.mark.integration
def test_deathstar_demo():
    ds = DeathstarDemo("deathstardemo-test", "dsd-out")
    ds.init_runtime()
    ds.runtime.run(run_async=True)
    print("Populating, press enter to go to the next step when done")
    ds.populate()

    client = DeathstarClient("deathstardemo-test", "dsd-out")
    input()
    print("testing user login")
    event = client.user_login()
    client.send(event)

    input()
    print("testing reserve")
    event = client.reserve()
    client.send(event)

    input()
    print("testing search")
    event = client.search_hotel()
    client.send(event)

    input()
    print("testing recommend (distance)")
    time.sleep(0.5)
    event = client.recommend(req_param="distance")
    client.send(event)

    input()
    print("testing recommend (price)")
    time.sleep(0.5)
    event = client.recommend(req_param="price")
    client.send(event)

    print(client.client._futures)
    input()
    print("done!")
    print(client.client._futures)


if __name__ == "__main__":
    test_deathstar_demo()