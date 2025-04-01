import sys
import os


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from cascade.dataflow.operator import StatefulOperator, StatelessOperator
from cascade.dataflow.dataflow import Event, InitClass, InvokeMethod, OpNode
from cascade.dataflow.optimization.dead_node_elim import dead_node_elimination
from cascade.runtime.python_runtime import PythonClientSync, PythonRuntime
from deathstar_movie_review.entities.compose_review import ComposeReview, compose_review_op
from deathstar_movie_review.entities.user import User, user_op
from deathstar_movie_review.entities.movie import MovieId, movie_id_op, movie_info_op, plot_op
from deathstar_movie_review.entities.frontend import frontend_op, text_op, unique_id_op, frontend_df_serial

import cascade

def init_python_runtime() -> tuple[PythonRuntime, PythonClientSync]:
    runtime = PythonRuntime()
    for op in cascade.core.operators.values():
        if isinstance(op, StatefulOperator):
            runtime.add_operator(op)
        elif isinstance(op, StatelessOperator):
            runtime.add_stateless_operator(op)
    
    runtime.run()
    return runtime, PythonClientSync(runtime)

import time
def test_deathstar_movie_demo_python():
    print("starting")
    cascade.core.clear() 
    exec(f'import deathstar_movie_review.entities.entities')
    cascade.core.init()

    runtime, client = init_python_runtime()
    user_op = cascade.core.operators["User"]
    compose_op = cascade.core.operators["ComposeReview"]
    movie_op = cascade.core.operators["MovieId"]
    frontend_op = cascade.core.operators["Frontend"]

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
    result = client.send(event)
    print(result)
    assert result.username == username

    print("testing compose review")
    req_id = "1"
    movie_title = "Cars 2"
    movie_id = 1

    # make the review
    event = compose_op.dataflows["__init__"].generate_event({"req_id": req_id}, req_id)
    result = client.send(event)
    print("review made")



    # # make the movie
    # init_movie = OpNode(MovieId, InitClass(), read_key_from="title")
    event = movie_op.dataflows["__init__"].generate_event({"title": movie_title, "movie_id": movie_id}, movie_title)
    result = client.send(event)
    # event = Event(init_movie, {"title": movie_title, "movie_id": movie_id}, None)
    # result = client.send(event)
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
    result = client.send(event, block=False)

    print(result)
    print("review composed")


    # read the review
    get_review = OpNode(ComposeReview, InvokeMethod("get_data"), read_key_from="req_id")
    event = Event(
        get_review,
        {"req_id": req_id},
        None
    )
    event = compose_op.dataflows["get_data"].generate_event({"req_id": req_id}, req_id)
    result = client.send(event)
    print(result)
    print(runtime.statefuloperators["ComposeReview"].states["1"].review_data)
    # time.sleep(0.5)

    # result = client.send(event)
    expected = {
        "userId": user_data["userId"],
        "movieId": movie_id,
        "text": review_data["text"]
    }
    # print(result, expected)
    assert "review_id" in result
    del result["review_id"] # randomly generated
    assert result == expected