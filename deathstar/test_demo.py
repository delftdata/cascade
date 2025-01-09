
from deathstar.demo import Client, DeathStarDemo
import time
import pytest

@pytest.mark.integration
def test_deathstar_demo():
    ds = DeathStarDemo("deathstardemo-test")
    ds.init_runtime()
    ds.runtime.run(run_async=True)
    print("Populating, press enter to go to the next step when done")
    ds.populate()

    client = Client("deathstardemo-test")
    input()
    print("testing user login")
    client.user_login()

    input()
    print("testing reserve")
    client.reserve()

    input()
    print("testing search")
    client.search_hotel()

    input()
    print("testing recommend (distance)")
    time.sleep(0.5)
    client.recommend(req_param="distance")

    input()
    print("testing recommend (price)")
    time.sleep(0.5)
    client.recommend(req_param="price")

    input()
    print("done!")


if __name__ == "__main__":
    test_deathstar_demo()