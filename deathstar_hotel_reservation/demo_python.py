import time
import sys
import os

# import cascade
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))


from cascade.runtime.python_runtime import PythonRuntime
from deathstar_hotel_reservation.demo import DeathstarDemo, deathstar_workload_generator
from timeit import default_timer as timer
import csv

messages_per_second = 10
sleeps_per_second = 10
sleep_time = 0.0085
seconds = 10

def benchmark_runner(runtime) -> dict[int, dict]:
    
    deathstar_generator = deathstar_workload_generator()
    # futures: dict[int, dict] = {}
    start = timer()
    for _ in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            event = next(deathstar_generator)
            # func_name = event.dataflow.name if event.dataflow is not None else "login" # only login has no dataflow
            key = event.key_stack[0]
            # params = event.variable_map
            runtime.send(event)
            # futures[event._id] = {"event": f'{func_name} {key}->{params}'}
        
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')
    end = timer()
    print(f'Average latency per second: {(end - start) / seconds}')
   
    # done = False
    # while not done:
    #     done = True
    #     for event_id, fut in client._futures.items():
    #         result = fut["ret"]
    #         if result is None:
    #             done = False
    #             time.sleep(0.5)
    #             break
    # futures = client._futures
    # client.close()
    # return futures


def write_dict_to_csv(futures_dict, filename):
    """
    Writes a dictionary of event data to a CSV file.

    Args:
        futures_dict (dict): A dictionary where each key is an event ID and the value is another dict.
        filename (str): The name of the CSV file to write to.
    """
    # Define the column headers
    headers = ["event_id", "sent", "sent_t", "ret", "ret_t", "latency"]
    
    # Open the file for writing
    with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        
        # Write the headers
        writer.writeheader()
        
        # Write the data rows
        for event_id, event_data in futures_dict.items():
            # Prepare a row where the 'event_id' is the first column
            row = {
                "event_id": event_id,
                "sent": event_data.get("sent"),
                "sent_t": event_data.get("sent_t"),
                "ret": event_data.get("ret"),
                "ret_t": event_data.get("ret_t"),
                "latency": event_data["ret_t"][1] - event_data["sent_t"][1]
            }
            writer.writerow(row)

def test_python_runtime():
    ds = DeathstarDemo()
    ds.init_runtime(PythonRuntime())
    ds.populate()


    time.sleep(1)
    input()

    results = benchmark_runner(ds.runtime)

    print(results)
    t = len(results)
    r = 0
    for result in results.values():
        if result["ret"] is not None:
            print(result)
            r += 1
    print(f"{r}/{t} results recieved.")
    write_dict_to_csv(results, "test2.csv")
