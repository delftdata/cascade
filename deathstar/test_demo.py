
from deathstar.demo import DeathStarDemo


def test_deathstar_demo():
    ds = DeathStarDemo()
    ds.init_runtime()
    ds.runtime.run(run_async=True)
    ds.populate()
    print("done!")

    input()
    print("user login")
    ds.user_login()

    input()
    print("reserve")
    ds.reserve()

    input()
    print("search")
    ds.search_hotel()

    input()
    print("recommend")
    ds.recommend(req_param="distance")

    input()
    print("recommend")
    ds.recommend(req_param="price")

    input()


if __name__ == "__main__":
    test_deathstar_demo()