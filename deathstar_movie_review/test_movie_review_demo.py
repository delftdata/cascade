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


def test_deathstar_movie_demo_python():
    print("starting")
    cascade.core.clear() 
    exec(f'import deathstar_movie_review.entities.entities')
    cascade.core.init()

    compose_df = cascade.core.dataflows[DataflowRef("Frontend", "compose")]
    df_parallel, _ = parallelize_until_if(compose_df)
    df_parallel.name = "compose_parallel"
    cascade.core.dataflows[DataflowRef("Frontend", "compose_parallel")] = df_parallel
    print(df_parallel.to_dot())
    assert len(df_parallel.entry) == 4

    runtime, client = init_python_runtime()
    deathstar_movie_demo(client)

@pytest.mark.integration
def test_deathstar_movie_demo_flink():
    print("starting")
    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")

    utils.create_topics()

    runtime = utils.init_flink_runtime("deathstar_movie_review.entities.entities")
    compose_df = cascade.core.dataflows[DataflowRef("Frontend", "compose")]
    df_parallel, _ = parallelize_until_if(compose_df)
    df_parallel.name = "compose_parallel"
    cascade.core.dataflows[DataflowRef("Frontend", "compose_parallel")] = df_parallel
    runtime.add_dataflow(df_parallel)
    print(df_parallel.to_dot())
    assert len(df_parallel.entry) == 4


    client = FlinkClientSync()
    runtime.run(run_async=True)

    try:
        deathstar_movie_demo(client)
    finally:
        client.close()

def deathstar_movie_demo(client):
    compose_df = cascade.core.dataflows[DataflowRef("Frontend", "compose")]
    
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

    event = cascade.core.dataflows[DataflowRef("User", "__init__")].generate_event({"username": username, "user_data": user_data}, username)
    result = client.send(event, block=True)
    print(result)
    assert result['username'] == username

    print("testing compose review")
    req_id = "4242"
    movie_title = "Cars 2"
    movie_id = 1

    # make the review
    event = cascade.core.dataflows[DataflowRef("ComposeReview", "__init__")].generate_event({"req_id": req_id}, req_id)
    result = client.send(event, block=True)
    print("review made")

    # # make the movie
    event = cascade.core.dataflows[DataflowRef("MovieId", "__init__")].generate_event({"title": movie_title, "movie_id": movie_id}, movie_title)
    result = client.send(event, block=True)
    print("movie made")

    # compose the review
    review_data = {
        "review": req_id,
        "user": username,
        "title": movie_title,
        "rating": None,
        "text": "good movie!"
    }

    r_data = {r+"_0": v for r, v in review_data.items()}

    event = compose_df.generate_event(r_data)
    result = client.send(event, block=True)
    print(result)
    print("review composed")


    event = cascade.core.dataflows[DataflowRef("ComposeReview", "get_data")].generate_event({"req_id": req_id}, req_id)
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



    ### PARALLEL ###
    df_parallel = cascade.core.dataflows[DataflowRef("Frontend", "compose_parallel")]


    # make the review
    new_req_id = "43"
    event = cascade.core.dataflows[DataflowRef("ComposeReview", "__init__")].generate_event({"req_id": new_req_id}, new_req_id)
    result = client.send(event, block=True)
    print("review made (parallel)")

    # compose the review
    review_data = {
        "review": req_id,
        "user": username,
        "title": movie_title,
        "rating": None,
        "text": "bad movie!"
    }

    r_data = {r+"_0": v for r, v in review_data.items()}

    event = df_parallel.generate_event(r_data)
    result = client.send(event, block=True)
    print(result)
    print("review composed (parallel)")

    event = cascade.core.dataflows[DataflowRef("ComposeReview", "get_data")].generate_event({"req_id": req_id}, req_id)
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


@pytest.mark.integration
def test_deathstar_movie_demo_prefetch_flink():
    print("starting")
    logger = logging.getLogger("cascade")
    logger.setLevel("DEBUG")

    utils.create_topics()



    runtime = utils.init_flink_runtime("deathstar_movie_review.entities.entities")
    
    # for prefetch experiment
    df_baseline_no = cascade.core.dataflows[DataflowRef("MovieId", "upload_movie_no_prefetch")]
    df_parallel_no = parallelize(df_baseline_no)
    df_parallel_no.name = "upload_movie_no_prefetch_parallel"
    cascade.core.dataflows[DataflowRef("MovieId", "upload_movie_no_prefetch_parallel")] = df_parallel_no
    runtime.add_dataflow(df_parallel_no)


     # for prefetch experiment
    df_baseline = cascade.core.dataflows[DataflowRef("MovieId", "upload_movie_prefetch")]
    df_parallel, _ = parallelize_until_if(df_baseline)
    df_parallel.name = "upload_movie_prefetch_parallel"
    cascade.core.dataflows[DataflowRef("MovieId", "upload_movie_prefetch_parallel")] = df_parallel
    runtime.add_dataflow(df_parallel)
    print(df_parallel.to_dot())
    assert len(df_parallel.entry) == 3


    client = FlinkClientSync()
    runtime.run(run_async=True)

    try:
        deathstar_prefetch(client)
    finally:
        client.close()

def deathstar_prefetch(client):
    event = cascade.core.dataflows[DataflowRef("MovieId", "__init__")].generate_event({"title": "cars", "movie_id": 1}, "cars")
    result = client.send(event, block=True)
    print("movie made")


    # make the review
    event = cascade.core.dataflows[DataflowRef("ComposeReview", "__init__")].generate_event({"req_id": "100"}, "100")
    result = client.send(event, block=True)
    print("review made")


    event = cascade.core.dataflows[DataflowRef("MovieId", "upload_movie_no_prefetch")].generate_event({"review_0": "100", "rating_0": 3}, "cars")
    result = client.send(event, block=True)
    print("movie uploaded")

    event = cascade.core.dataflows[DataflowRef("MovieId", "upload_movie_prefetch_parallel")].generate_event({"review_0": "100", "rating_0": 3}, "cars")
    result = client.send(event, block=True)
    print("movie uploaded w/ prefetch")
    print(result)

    event = cascade.core.dataflows[DataflowRef("MovieId", "upload_movie_no_prefetch_parallel")].generate_event({"review_0": "100", "rating_0": 3}, "cars")
    result = client.send(event, block=True)
    print("movie uploaded")