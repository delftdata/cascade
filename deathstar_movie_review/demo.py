import hashlib
import uuid

from .movie_data import movie_data
from .workload_data import movie_titles, charset
import random
from timeit import default_timer as timer
import sys
import os

# import cascade
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from cascade.dataflow.dataflow import Event, EventResult, InitClass, OpNode
from cascade.runtime.flink_runtime import FlinkClientSync, FlinkRuntime
from cascade.dataflow.optimization.dead_node_elim import dead_node_elimination

from .entities.user import user_op, User
from .entities.compose_review import compose_review_op
from .entities.frontend import frontend_op, text_op, unique_id_op
from .entities.movie import MovieInfo, movie_id_op, movie_info_op, plot_op, Plot, MovieId

import time
import pandas as pd

def populate_user(client: FlinkRuntime):
    init_user = OpNode(User, InitClass(), read_key_from="username")
    for i in range(1000):
        user_id = f'user{i}'
        username = f'username_{i}'
        password = f'password_{i}'
        hasher = hashlib.new('sha512')
        salt = uuid.uuid1().bytes
        hasher.update(password.encode())
        hasher.update(salt)

        password_hash = hasher.hexdigest()

        user_data = {
            "userId": user_id,
            "FirstName": "firstname",
            "LastName": "lastname",
            "Username": username,
            "Password": password_hash,
            "Salt": salt
        }
        event = Event(init_user, {"username": username, "user_data": user_data}, None)
        client.send(event)


def populate_movie(client: FlinkRuntime):
    init_movie_info = OpNode(MovieInfo, InitClass(), read_key_from="movie_id")
    init_plot = OpNode(Plot, InitClass(), read_key_from="movie_id")
    init_movie_id = OpNode(MovieId, InitClass(), read_key_from="title")
    
    for movie in movie_data:
        movie_id = movie["MovieId"]

        # movie info -> write `movie`
        event = Event(init_movie_info, {"movie_id": movie_id, "info": movie}, None)
        client.send(event)

        # plot -> write "plot"
        event = Event(init_plot, {"movie_id": movie_id, "plot": "plot"}, None)
        client.send(event)

        # movie_id_op -> register movie id 
        event = Event(init_movie_id, {"title": movie["Title"], "movie_id": movie_id}, None)
        client.send(event)


def compose_review(req_id):
    user_index = random.randint(0, 999)
    username = f"username_{user_index}"
    password = f"password_{user_index}"
    title = random.choice(movie_titles)
    rating = random.randint(0, 10)
    text = ''.join(random.choice(charset) for _ in range(256))
    
    return frontend_op.dataflow.generate_event({
            "review": req_id,
            "user": username,
            "title": title,
            "rating": rating,
            "text": text
        })

def deathstar_workload_generator():
    c = 1
    while True:
        yield compose_review(c)
        c += 1

threads = 1
messages_per_burst = 10
sleeps_per_burst = 10
sleep_time = 0.08 #0.0085
seconds_per_burst = 1
bursts = 100


def benchmark_runner(proc_num) -> dict[int, dict]:
    print(f'Generator: {proc_num} starting')
    client = FlinkClientSync("ds-movie-in", "ds-movie-out")
    deathstar_generator = deathstar_workload_generator()
    start = timer()
    
    for _ in range(bursts):
        sec_start = timer()

        # send burst of messages
        for i in range(messages_per_burst):

            # sleep sometimes between messages
            if i % (messages_per_burst // sleeps_per_burst) == 0:
                time.sleep(sleep_time)
            event = next(deathstar_generator)
            client.send(event)
        
        client.flush()
        sec_end = timer()

        # wait out the second
        lps = sec_end - sec_start
        if lps < seconds_per_burst:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per burst: {sec_end2 - sec_start} ({seconds_per_burst})')
        
    end = timer()
    print(f'Average latency per burst: {(end - start) / bursts} ({seconds_per_burst})')
    
    done = False
    while not done:
        done = True
        for event_id, fut in client._futures.items():
            result = fut["ret"]
            if result is None:
                done = False
                time.sleep(0.5)
                break
    futures = client._futures
    client.close()
    return futures


def write_dict_to_pkl(futures_dict, filename):
    """
    Writes a dictionary of event data to a pickle file.

    Args:
        futures_dict (dict): A dictionary where each key is an event ID and the value is another dict.
        filename (str): The name of the pickle file to write to.
    """

    # Prepare the data for the DataFrame
    data = []
    for event_id, event_data in futures_dict.items():
        ret: EventResult = event_data.get("ret")
        row = {
            "event_id": event_id,
            "sent": str(event_data.get("sent")),
            "sent_t": event_data.get("sent_t"),
            "ret": str(event_data.get("ret")),
            "ret_t": event_data.get("ret_t"),
            "roundtrip": ret.metadata["roundtrip"] if ret else None,
            "flink_time": ret.metadata["flink_time"] if ret else None,
            "deser_times": ret.metadata["deser_times"] if ret else None,
            "loops": ret.metadata["loops"] if ret else None,
            "latency": event_data["ret_t"][1] - event_data["sent_t"][1] if ret else None
        }
        data.append(row)

    # Create a DataFrame and save it as a pickle file
    df = pd.DataFrame(data)
    df.to_pickle(filename)

def main():
    runtime = FlinkRuntime("ds-movie-in", "ds-movie-out", 8081)
    runtime.init(bundle_time=5, bundle_size=10)

    print(frontend_op.dataflow.to_dot())
    # dead_node_elimination([], [frontend_op])
    print(frontend_op.dataflow.to_dot())
    input()


    runtime.add_operator(compose_review_op)
    runtime.add_operator(user_op)
    runtime.add_operator(movie_info_op)
    runtime.add_operator(movie_id_op)
    runtime.add_operator(plot_op)
    runtime.add_stateless_operator(frontend_op)
    runtime.add_stateless_operator(unique_id_op)
    runtime.add_stateless_operator(text_op)

    runtime.run(run_async=True)
    populate_user(runtime)
    populate_movie(runtime)
    runtime.producer.flush()
    time.sleep(1)

    input()

    # with Pool(threads) as p:
    #     results = p.map(benchmark_runner, range(threads))

    # results = {k: v for d in results for k, v in d.items()}
    results = benchmark_runner(0)

    print("last result:")
    print(list(results.values())[-1])
    t = len(results)
    r = 0
    for result in results.values():
        if result["ret"] is not None:
            print(result)
            r += 1
    print(f"{r}/{t} results recieved.")
    write_dict_to_pkl(results, "test2.pkl")

if __name__ == "__main__":
    main()

