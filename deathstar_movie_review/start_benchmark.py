import hashlib
from multiprocessing import Pool
import time
from typing import Literal
import uuid
import pandas as pd
import random


from .movie_data import movie_data
from .workload_data import movie_titles, charset
import sys
import os
from timeit import default_timer as timer
import argparse

# import cascade
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from tests.integration.flink.utils import init_cascade_from_module, init_flink_runtime
import cascade
from cascade.dataflow.optimization.parallelization import parallelize_until_if
from cascade.dataflow.dataflow import DataflowRef,EventResult
from cascade.runtime.flink_runtime import FlinkClientSync

IN_TOPIC = "ds-movie-in"
OUT_TOPIC = "ds-movie-out"
# threads = 1
# messages_per_burst = 10
# sleeps_per_burst = 10
# sleep_time = 0.08 
# seconds_per_burst = 1
# bursts = 100

def populate_user(client: FlinkClientSync):
    user_init = cascade.core.dataflows[DataflowRef("User", "__init__")]
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
        event = user_init.generate_event({"username": username, "user_data": user_data}, key=username)
        client.send(event)


def populate_movie(client: FlinkClientSync):
    movieinfo_init = cascade.core.dataflows[DataflowRef("MovieInfo", "__init__")]
    plot_init = cascade.core.dataflows[DataflowRef("Plot", "__init__")]
    movieid_init = cascade.core.dataflows[DataflowRef("MovieId", "__init__")]

    for movie in movie_data:
        movie_id = movie["MovieId"]

        # movie info -> write `movie`
        event = movieinfo_init.generate_event({"movie_id": movie_id, "info": movie}, key=movie_id)
        client.send(event)

        # plot -> write "plot"
        event = plot_init.generate_event({"movie_id": movie_id, "plot": "plot"}, key=movie_id)
        client.send(event)

        # movie_id_op -> register movie id 
        event = movieid_init.generate_event({"title": movie["Title"], "movie_id": movie_id}, key=movie["Title"])
        client.send(event)


def compose_review(req_id, parallel=False):
    user_index = random.randint(0, 999)
    username = f"username_{user_index}"
    password = f"password_{user_index}"
    title = random.choice(movie_titles)
    rating = None
    # rating = random.randint(0, 10)
    text = ''.join(random.choice(charset) for _ in range(256))
    
    if parallel:
        compose = cascade.core.dataflows[DataflowRef("Frontend", "compose_parallel")]
    else:
        compose = cascade.core.dataflows[DataflowRef("Frontend", "compose")]

    return compose.generate_event({
            "req_id": req_id, # hacky way to create the compose review object when it doesn't exist
            "review_0": req_id,
            "user_0": username,
            "title_0": title,
            "rating_0": rating,
            "text_0": text
        })

def deathstar_workload_generator(parallel=False):
    c = 1
    while True:
        yield compose_review(c, parallel)
        c += 1


def benchmark_runner(args) -> dict[int, dict]:
    proc_num, requests_per_second, sleep_time, bursts, parallel = args
    print(f'Generator: {proc_num} starting')
    client = FlinkClientSync(IN_TOPIC, OUT_TOPIC)
    deathstar_generator = deathstar_workload_generator(parallel)
    start = timer()
    
    for b in range(bursts):
        sec_start = timer()

        # send burst of messages
        for i in range(requests_per_second):

            # sleep sometimes between messages
            # if i % (messages_per_burst // sleeps_per_burst) == 0:
            time.sleep(sleep_time)
            event = next(deathstar_generator)
            client.send(event)
        
        client.flush()
        sec_end = timer()

        # wait out the second
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per burst: {sec_end2 - sec_start} ({b+1}/{bursts})')
        
    end = timer()
    avg_send_latency = (end - start) / bursts
    print(f'Average send latency per burst for generator {proc_num} was: {avg_send_latency}')
    if avg_send_latency > 1.1:
        print(f'This is higher than expected (1). Maybe increase the number of threads?')
    futures = wait_for_futures(client)
    client.close()
    return futures

def wait_for_futures(client: FlinkClientSync):   
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
    
    # Multiply flink_time by 1000 to convert to milliseconds
    df['flink_time'] = df['flink_time'] * 1000

    return df

def main():
    parser = argparse.ArgumentParser(description="Run the benchmark and save results.")
    parser.add_argument("-o", "--output", type=str, default="benchmark_results.pkl", help="Output file name for the results")
    parser.add_argument("--requests_per_second", type=int, default=10, help="Number of messages per burst")
    parser.add_argument("--seconds", type=int, default=100, help="Number of seconds to benchmark for")
    parser.add_argument("--threads", type=int, default=1, help="Number of concurrent threads")
    parser.add_argument("--experiment", type=str, default="baseline", help="Experiment type")
    parser.add_argument("--no_init", action="store_true", help="Don't populate")
    args = parser.parse_args()
    
    rps_per_thread = int(args.requests_per_second / args.threads)
    sleep_time = 0.95 / rps_per_thread

    EXPERIMENT = args.experiment
    
    print(f"Experiment [{EXPERIMENT}]")
    print(f"Starting with args:\n{args}")
    print(f"Actual requests per second is {int(rps_per_thread * args.threads)} (due to rounding)")

    init_cascade_from_module("deathstar_movie_review.entities.entities")

    init_client = FlinkClientSync(IN_TOPIC, OUT_TOPIC)

    df_baseline = cascade.core.dataflows[DataflowRef("Frontend", "compose")]
    print(df_baseline.to_dot())
    df_parallel, _ = parallelize_until_if(df_baseline)
    df_parallel.name = "compose_parallel"
    cascade.core.dataflows[DataflowRef("Frontend", "compose_parallel")] = df_parallel
    print(cascade.core.dataflows.keys())

    for df in cascade.core.dataflows.values():
        print(df.to_dot())
        for block in df.blocks.values():
            print(block.function_string)
                    
    if not args.no_init:
        print("Populating...")
        populate_user(init_client)
        populate_movie(init_client)
        init_client.producer.flush()
        wait_for_futures(init_client)
        print("Done.")
        time.sleep(1)

    print("Starting benchmark")
    parallel = args.experiment == "parallel"

    func_args = [(t, rps_per_thread, sleep_time, args.seconds, parallel) for t in range(args.threads)]
    with Pool(args.threads) as p:
        results = p.map(benchmark_runner, func_args)

    results = {k: v for d in results for k, v in d.items()}

    print("last result:")
    print(list(results.values())[-1])
    t = len(results)
    r = 0
    for result in results.values():
        if result["ret"] is not None:
            r += 1

    print(f"{r}/{t} results recieved.")
    print(f"Writing results to {args.output}")

    df = write_dict_to_pkl(results, args.output)

    flink_time = df['flink_time'].median()
    latency = df['latency'].median()
    flink_prct = float(flink_time) * 100 / latency
    print(f"Median latency    : {latency:.2f} ms")
    print(f"Median Flink time : {flink_time:.2f} ms ({flink_prct:.2f}%)")
    init_client.close()

    df = preprocess(args.output, df)
    df.to_pickle(args.output)
    

import re

def preprocess(name, df, warmup_time_s=3) -> pd.DataFrame:
    # Extract parallelism and mps from the name using regex
    match = re.search(r'(.+)_p-(\d+)_rps-(\d+)', name)
    if match:
        experiment = match.group(1)
        parallelism = int(match.group(2))
        mps = int(match.group(3))
    else:
        raise Exception()
    
    # Ignore the first warmup_time seconds of events
    warmup_events = int(warmup_time_s * mps)
    df = df.iloc[warmup_events:]

    # Calculate the additional Kafka overhead
    # df['kafka_overhead'] = df['latency'] - df['flink_time']

    # Extract median values from df
    flink_time_median = df['flink_time'].median()
    latency_median = df['latency'].median()
    flink_time_99_percentile = df['flink_time'].quantile(0.99)
    latency_99_percentile = df['latency'].quantile(0.99)
    flink_time_95_percentile = df['flink_time'].quantile(0.95)
    latency_95_percentile = df['latency'].quantile(0.95)

    data = {
        'experiment': experiment,
        'parallelism': parallelism,
        'mps': mps,
        'flink_time_median': flink_time_median,
        'latency_median': latency_median,
        'latency_99_percentile': latency_99_percentile,
        'latency_95_percentile': latency_95_percentile,
        'flink_time_99_percentile': flink_time_99_percentile,
        'flink_time_95_percentile': flink_time_95_percentile
    }
    data = {k:[v] for k,v in data.items()} 
    return pd.DataFrame(data)

if __name__ == "__main__":
    main()