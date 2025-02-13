from cascade.dataflow.dataflow import Event, InitClass, InvokeMethod, OpNode
from cascade.dataflow.optimization.dead_node_elim import dead_node_elimination
from cascade.runtime.python_runtime import PythonClientSync, PythonRuntime
from deathstar_movie_review.entities.compose_review import ComposeReview, compose_review_op
from deathstar_movie_review.entities.user import User, user_op
from deathstar_movie_review.entities.movie import MovieId, movie_id_op, movie_info_op, plot_op
from deathstar_movie_review.entities.frontend import frontend_op, text_op, unique_id_op



def test_deathstar_movie_demo_python():
    print("starting")
    runtime = PythonRuntime()
    
    print(frontend_op.dataflow.to_dot())
    dead_node_elimination([], [frontend_op])
    print(frontend_op.dataflow.to_dot())

    runtime.add_operator(compose_review_op)
    runtime.add_operator(user_op)
    runtime.add_operator(movie_info_op)
    runtime.add_operator(movie_id_op)
    runtime.add_operator(plot_op)
    runtime.add_stateless_operator(frontend_op)
    runtime.add_stateless_operator(unique_id_op)
    runtime.add_stateless_operator(text_op)

    runtime.run()
    client = PythonClientSync(runtime)

    init_user = OpNode(User, InitClass(), read_key_from="username")
    username = "username_1"
    user_data = {
            "userId": "user1",
            "FirstName": "firstname",
            "LastName": "lastname",
            "Username": username,
            "Password": "****",
            "Salt": "salt"
        }
    print("testing user create")
    event = Event(init_user, {"username": username, "user_data": user_data}, None)
    result = client.send(event)
    assert isinstance(result, User) and result.username == username

    print("testing compose review")
    req_id = 1
    movie_title = "Cars 2"
    movie_id = 1

    # make the review
    init_compose_review = OpNode(ComposeReview, InitClass(), read_key_from="req_id")
    event = Event(init_compose_review, {"req_id": req_id}, None)
    result = client.send(event)
    print("review made")


    # make the movie
    init_movie = OpNode(MovieId, InitClass(), read_key_from="title")
    event = Event(init_movie, {"title": movie_title, "movie_id": movie_id}, None)
    result = client.send(event)
    print("movie made")

    # compose the review
    review_data = {
        "review": req_id,
        "user": username,
        "title": movie_title,
        "rating": 5,
        "text": "good movie!"
    }

    event = Event(
        frontend_op.dataflow.entry, 
        review_data,
        frontend_op.dataflow)
    result = client.send(event)
    print(result)
    print("review composed")


    # read the review
    get_review = OpNode(ComposeReview, InvokeMethod("get_data"), read_key_from="req_id")
    event = Event(
        get_review,
        {"req_id": req_id},
        None
    )
    result = client.send(event)
    expected = {
        "userId": user_data["userId"],
        "movieId": movie_id,
        "text": review_data["text"]
    }
    print(result, expected)
    assert "review_id" in result
    del result["review_id"] # randomly generated
    assert result == expected

    print("Success!")
    