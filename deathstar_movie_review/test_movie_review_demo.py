import logging
import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from cascade.dataflow.optimization.parallelization import parallelize
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


def test_deathstar_movie_demo_python():
    print("starting")
    cascade.core.clear() 
    exec(f'import deathstar_movie_review.entities.entities')
    cascade.core.init()

    runtime, client = init_python_runtime()
    deathstar_movie_demo(client)

@pytest.mark.integration
def test_deathstar_movie_demo_flink():
    print("starting")
    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")

    utils.create_topics()

    runtime, client = utils.init_flink_runtime("deathstar_movie_review.entities.entities")
    runtime.run(run_async=True)

    try:
        deathstar_movie_demo(client)
    finally:
        client.close()

def deathstar_movie_demo(client):
    user_op = cascade.core.operators["User"]
    compose_op = cascade.core.operators["ComposeReview"]
    movie_op = cascade.core.operators["MovieId"]
    frontend_op = cascade.core.operators["Frontend"]
    df = parallelize(frontend_op.dataflows["compose"])
    df.name = "compose_parallel"
    frontend_op.dataflows["compose_parallel"] = df
    print(frontend_op.dataflows["compose_parallel"].to_dot())
    print(frontend_op.dataflows)
    assert len(frontend_op.dataflows["compose_parallel"].entry) == 4

    for df in cascade.core.dataflows.values():
        print(df.to_dot())

    username = "myUsername"
    user_data = {
            "userId": "user1",
            "FirstName": "firstname",
            "LastName": "lastname",
            "Username": username,
            "Password": "****",
            "Salt": "salt"
        }
    
    print("testing user create")

    event = user_op.dataflows["__init__"].generate_event({"username": username, "user_data": user_data}, username)
    result = client.send(event, block=True)
    print(result)
    assert result.username == username

    print("testing compose review")
    req_id = "4242"
    movie_title = "Cars 2"
    movie_id = 1

    # make the review
    event = compose_op.dataflows["__init__"].generate_event({"req_id": req_id}, req_id)
    result = client.send(event, block=True)
    print("review made")

    # # make the movie
    # init_movie = OpNode(MovieId, InitClass(), read_key_from="title")
    event = movie_op.dataflows["__init__"].generate_event({"title": movie_title, "movie_id": movie_id}, movie_title)
    result = client.send(event, block=True)
    print("movie made")

    # compose the review
    review_data = {
        "review": req_id,
        "user": username,
        "title": movie_title,
        "rating": 5,
        "text": "good movie!"
    }

    r_data = {r+"_0": v for r, v in review_data.items()}

    event = frontend_op.dataflows["compose"].generate_event(r_data)
    result = client.send(event, block=True)
    print(result)
    print("review composed")


    event = compose_op.dataflows["get_data"].generate_event({"req_id": req_id}, req_id)
    result = client.send(event, block=True)
    print(result)

    expected = {
        "userId": user_data["userId"],
        "movieId": movie_id,
        "text": review_data["text"]
    }

    assert "review_id" in result
    del result["review_id"] # randomly generated
    assert result == expected



    ## NOW DO IT PARALLEL!
    # make the review
    new_req_id = "43"
    event = compose_op.dataflows["__init__"].generate_event({"req_id": new_req_id}, new_req_id)
    result = client.send(event, block=True)
    print("review made")

    # compose the review
    review_data = {
        "review": req_id,
        "user": username,
        "title": movie_title,
        "rating": 2,
        "text": "bad movie!"
    }

    r_data = {r+"_0": v for r, v in review_data.items()}

    event = frontend_op.dataflows["compose_parallel"].generate_event(r_data)
    result = client.send(event, block=True)
    print(result)
    print("review composed (parallel)")

    event = compose_op.dataflows["get_data"].generate_event({"req_id": req_id}, req_id)
    result = client.send(event, block=True)
    print(result)

    expected = {
        "userId": user_data["userId"],
        "movieId": movie_id,
        "text": "bad movie!"
    }

    assert "review_id" in result
    del result["review_id"] # randomly generated
    assert result == expected