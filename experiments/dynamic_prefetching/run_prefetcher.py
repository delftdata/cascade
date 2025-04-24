import argparse
import logging
from multiprocessing import Pool
import sys
import os
from typing import Counter, Literal
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import cascade
from cascade.dataflow.optimization.parallelization import parallelize
from cascade.runtime.flink_runtime import FlinkClientSync
from cascade.dataflow.dataflow import DataFlow, DataflowRef, EventResult
from tests.integration.flink.utils import create_topics, init_cascade_from_module, init_flink_runtime, wait_for_event_id
from timeit import default_timer as timer


KAFKA_BROKER = "localhost:9092"
KAFKA_FLINK_BROKER = "kafka:9093" # If running a flink cluster and kafka inside docker, the broker url might be different

IN_TOPIC = "prefetcher-in"
OUT_TOPIC = "prefetcher-out"
INTERNAL_TOPIC = "prefetcher-internal"

def main():
    init_cascade_from_module("experiments.dynamic_prefetching.entities")

    

    # logger = logging.getLogger("cascade")
    # logger.setLevel("DEBUG")
    # runtime = init_flink_runtime("experiments.dynamic_prefetching.entities", parallelism=4)

    print(cascade.core.dataflows.keys())

    baseline = cascade.core.dataflows[DataflowRef("Prefetcher", "baseline")]
    prefetch = cascade.core.dataflows[DataflowRef("Prefetcher", "prefetch")]


    pre_par = parallelize(prefetch)
    cascade.core.dataflows[DataflowRef("Prefetcher", "prefetch_parallel")] = pre_par

    base_par = parallelize(baseline)
    cascade.core.dataflows[DataflowRef("Prefetcher", "baseline_parallel")] = base_par

    print(base_par.to_dot())
    print(pre_par.to_dot())

    run_test()


import time
def wait_for_futures(client: FlinkClientSync): 
    print("waiting")  
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

def generate_event(exp: Literal["baseline", "prefetch"], chance: float):
    baseline = cascade.core.dataflows[DataflowRef("Prefetcher", "baseline_parallel")]
    prefetch = cascade.core.dataflows[DataflowRef("Prefetcher", "prefetch_parallel")]
    df = prefetch if exp == "prefetch" else baseline

    return df.generate_event({"branch_chance_0": chance})

def runner(args):
    chance, bursts, requests_per_second, exp = args
    client = FlinkClientSync(IN_TOPIC, OUT_TOPIC)
    sleep_time = 0.95 / requests_per_second

    start = timer()
    for b in range(bursts):
        sec_start = timer()

        # send burst of messages
        for i in range(requests_per_second):

            # sleep sometimes between messages
            # if i % (messages_per_burst // sleeps_per_burst) == 0:
            time.sleep(sleep_time)
            event = generate_event(exp, chance)
            client.send(event)
        
        client.flush()
        sec_end = timer()

        # wait out the second
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)

    end = timer()
    avg_send_latency = (end - start) / bursts
    print(f'Average send latency per burst for generator was: {avg_send_latency}')
    if avg_send_latency > 1.1:
        print(f'This is higher than expected (1). Maybe increase the number of threads?')
    futures = wait_for_futures(client)
    client.close()
    return futures

def run_test():
    logger = logging.getLogger("cascade")
    logger.setLevel("INFO")
    
    

    parser = argparse.ArgumentParser(description="Run the benchmark and save results.")
    parser.add_argument("--requests_per_second", type=int, default=10, help="Number of messages per burst")
    parser.add_argument("--seconds", type=int, default=100, help="Number of seconds to benchmark for")
    parser.add_argument("--threads", type=int, default=1, help="Number of concurrent threads")
    parser.add_argument("--chance", type=float, default=0.5, help="Chance")
    parser.add_argument("--experiment", type=str, default="baseline", help="Experiment type")
    args = parser.parse_args()

    assert args.experiment in ["baseline", "prefetch"]
    rps_per_thread = int(args.requests_per_second / args.threads)
    print(f"{args.chance} - {args.experiment}: {args.requests_per_second} rps for {args.seconds}s")
    print(f"Actual requests per second is {int(rps_per_thread * args.threads)} (due to rounding)")


    func_args = [(args.chance, args.seconds,rps_per_thread,args.experiment)]
    with Pool(args.threads) as p:
        results = p.map(runner, func_args)

    results = {k: v for d in results for k, v in d.items()}

    count = Counter([r["ret"].result for r in results.values()])
    print(count)
    df = to_pandas(results)
    df.to_csv(f"{args.experiment}_{args.chance}_{args.requests_per_second}.csv")
        


def to_pandas(futures_dict):
    # Prepare the data for the DataFrame
    data = []
    for event_id, event_data in futures_dict.items():
        ret: EventResult = event_data.get("ret")
        row = {
            "event_id": event_id,
            "result": ret.result if ret else None,
            "flink_time": ret.metadata["flink_time"] if ret else None,
            "loops": ret.metadata["loops"] if ret else None,
            "latency": event_data["ret_t"][1] - event_data["sent_t"][1] if ret else None
        }
        data.append(row)

    # Create a DataFrame and save it as a pickle file
    df = pd.DataFrame(data)
    
    # Multiply flink_time by 1000 to convert to milliseconds
    df['flink_time'] = df['flink_time'] * 1000
    flink_time = df['flink_time'].median()
    latency = df['latency'].median()
    flink_prct = float(flink_time) * 100 / latency
    print(f"Median latency    : {latency:.2f} ms")
    print(f"Median Flink time : {flink_time:.2f} ms ({flink_prct:.2f}%)")

    latency = df['latency'].mean()
    print(f"Mean latency    : {latency:.2f} ms")


    return df

if __name__ == "__main__":
    main()