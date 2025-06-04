import argparse
from multiprocessing import Pool
import random
from timeit import default_timer as timer
import time
import pandas as pd
import sys
import os

# import cascade
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from cascade.runtime.flink_runtime import FlinkClientSync
from cascade.dataflow.dataflow import DataflowRef, EventResult
import cascade

from tests.integration.flink.utils import init_cascade_from_module

IN_TOPIC = "ds-hotel-in"
OUT_TOPIC = "ds-hotel-out"


AMOUNT_OF_USERS = 500
AMOUNT_OF_HOTELS = 80

def populate_users(client: FlinkClientSync):
    # populate users
    user_init = cascade.core.dataflows[DataflowRef("User", "__init__")]
    for i in range(AMOUNT_OF_USERS):
        username = f"Delft_{i}"
        password = str(i) * 10
        client.send(user_init.generate_event({"username": username, "password": password}, key=username))

def populate_hotels(client: FlinkClientSync):
    # populate hotels
    reservation_init = cascade.core.dataflows[DataflowRef("Reservation", "__init__")]
    add_reservation = cascade.core.dataflows[DataflowRef("Reservation", "add_reservation")]
    client.send(reservation_init.generate_event({"hotel_id": "1", "capacity": 200}, key="1"))
    client.send(reservation_init.generate_event({"hotel_id": "2", "capacity": 200}, key="2"))
    client.send(reservation_init.generate_event({"hotel_id": "3", "capacity": 200}, key="3"))
    client.send(reservation_init.generate_event({"hotel_id": "5", "capacity": 200}, key="5"))
    client.send(reservation_init.generate_event({"hotel_id": "6", "capacity": 200}, key="6"))
    client.send(reservation_init.generate_event({"hotel_id": "4", "capacity": 200}, key="4"), block=True)

    client.send(add_reservation.generate_event({
        "customer_name": "Alice",
        "in_date": "2015-04-09",
        "out_date": "2015-04-10",
        "number_of_rooms": 1
        }, key="4"))
    
    for i in range(7, AMOUNT_OF_HOTELS + 1):
        max_capacity = 200

        if i % 3 == 1:
            max_capacity = 300
        elif i % 3 == 2:
            max_capacity = 250

        client.send(reservation_init.generate_event({"hotel_id": str(i), "capacity": max_capacity}, key=str(i)))


def user_login(client: FlinkClientSync):
    username, password = get_user() 
    user_login = cascade.core.dataflows[DataflowRef("User", "login")]
    event = user_login.generate_event({"password_0": password}, key=username)
    client.send(event)

def reserve( 
    in_date: str,
    out_date: str,
    hotel_id: str,
    customer_name: str,
    amount_of_rooms: int,
    username: str,
    password: str,
    client: FlinkClientSync):
    
    # login first
    df = cascade.core.dataflows[DataflowRef("User", "login")]
    login_success = client.send(df.generate_event({"password_0": password}, key=username), block=True)

    if not login_success:
        raise ValueError("Login failed")
    
    # reserve
    df_reserve = cascade.core.dataflows[DataflowRef("Reservation", "add_reservation")]
    event = df_reserve.generate_event({
        "customer_name_0": customer_name, 
        "in_date_0": in_date, 
        "out_date_0": out_date, 
        "amount_of_rooms_0": amount_of_rooms
        }, key=hotel_id)
    reserve_success = client.send(event)

    if reserve_success:
        return "Success"
    else:
        return "Failed reservation"

def get_user():
    i = random.randint(0, AMOUNT_OF_USERS-1)
    return f"Delft_{i}", str(i) * 10

def do_reserve(client):
    in_date = random.randint(9, 23)
    out_date = in_date + random.randint(1, 5)

    if in_date <= 9:
        in_date_str = "2015-04-0" + str(in_date)
    else:
        in_date_str = "2015-04-" + str(in_date)

    if out_date <= 9:
        out_date_str = "2015-04-0" + str(out_date)
    else:
        out_date_str = "2015-04-" + str(out_date)

    hotel_id = str(random.randint(1, AMOUNT_OF_HOTELS))
    user_id, password = get_user()
    customer_name = user_id
    num_rooms = 1

    reserve(in_date_str, out_date_str, hotel_id, customer_name, num_rooms, user_id, password, client)

# def deathstar_workload_generator():
#     search_ratio = 0.6
#     recommend_ratio = 0.39
#     user_ratio = 0.005
#     reserve_ratio = 0.005
#     c = 0
#     while True:
#         coin = random.random()
#         if coin < search_ratio:
#             yield search_hotel()
#         elif coin < search_ratio + recommend_ratio:
#             yield recommend()
#         elif coin < search_ratio + recommend_ratio + user_ratio:
#             yield user_login()
#         else:
#             yield reserve()
#         c += 1

def reserve_workload_generator(client):
    do_reserve(client)

def user_login_workload_generator(client):
    user_login(client)


def benchmark_runner(args) -> dict[int, dict]:
    proc_num, requests_per_second, sleep_time, bursts = args
    print(f'Generator: {proc_num} starting')
    client = FlinkClientSync(IN_TOPIC, OUT_TOPIC)
    deathstar_generator = user_login_workload_generator
    start = timer()
    
    for b in range(bursts):
        sec_start = timer()

        # send burst of messages
        for i in range(requests_per_second):

            # sleep sometimes between messages
            # if i % (messages_per_burst // sleeps_per_burst) == 0:
            time.sleep(sleep_time)
            deathstar_generator(client)
        
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
    parser.add_argument("--rps", type=int, default=10, help="Number of messages per burst")
    parser.add_argument("--seconds", type=int, default=100, help="Number of seconds to benchmark for")
    parser.add_argument("--threads", type=int, default=1, help="Number of concurrent threads")
    parser.add_argument("--no_init", action="store_true", help="Don't populate")
    args = parser.parse_args()
    
    
    rps_per_thread = int(args.rps / args.threads)
    sleep_time = 0.95 / rps_per_thread

    
    print(f"Starting with args:\n{args}")
    print(f"Actual requests per second is {int(rps_per_thread * args.threads)} (due to rounding)")

    init_cascade_from_module([
        "deathstar_hotel_reservation.entities.user",
        # "deathstar_hotel_reservation.entities.reservation",
        ])

    init_client = FlinkClientSync(IN_TOPIC, OUT_TOPIC)

    print(cascade.core.dataflows.keys())

    for df in cascade.core.dataflows.values():
        print(df.to_dot())
        for block in df.blocks.values():
            print(block.function_string)
                    
    if not args.no_init:
        print("Populating...")
        populate_users(init_client)
        # populate_hotels(init_client)
        init_client.producer.flush()
        wait_for_futures(init_client)
        print("Done.")
        time.sleep(1)

    print("Starting benchmark")

    func_args = [(t, rps_per_thread, sleep_time, args.seconds) for t in range(args.threads)]
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
    df.to_csv(args.output)

import re

def preprocess(name, df, warmup_time_s=3) -> pd.DataFrame:
    # Extract parallelism and mps from the name using regex
    match = re.search(r'(\d+)x(\d+)_rps-(\d+)', name)
    if match:
        tm = int(match.group(1))
        slots = int(match.group(2))
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
        'taskmanagers': tm,
        'slots': slots,
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