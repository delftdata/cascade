import random
import sys
import os
import time
import csv
from timeit import default_timer as timer
from multiprocessing import Pool

# import cascade
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from cascade.dataflow.dataflow import Event, EventResult, InitClass, InvokeMethod, OpNode
from cascade.runtime.flink_runtime import FlinkClientSync, FlinkOperator, FlinkRuntime, FlinkStatelessOperator
from deathstar.entities.flight import Flight, flight_op
from deathstar.entities.hotel import Geo, Hotel, Rate, hotel_op
from deathstar.entities.recommendation import Recommendation, recommend_op
from deathstar.entities.search import Search, search_op
from deathstar.entities.user import User, user_op
import pandas as pd


class DeathstarDemo():
    def __init__(self):
        self.init_user = OpNode(User, InitClass(), read_key_from="user_id")
        self.init_hotel = OpNode(Hotel, InitClass(), read_key_from="key")
        self.init_flight = OpNode(Flight, InitClass(), read_key_from="id")
    
    def init_runtime(self, runtime, **kwargs):
        self.runtime = runtime
        self.runtime.init(**kwargs)
        self.runtime.add_operator(hotel_op)
        self.runtime.add_operator(flight_op)
        self.runtime.add_operator(user_op)
        self.runtime.add_stateless_operator(search_op)
        self.runtime.add_stateless_operator(recommend_op)


    def populate(self):
        # Create locations & rates for hotels
        geos = []
        geos.append(Geo(37.7867, 0))
        geos.append(Geo(37.7854, -122.4005))
        geos.append(Geo(37.7867, -122.4071))
        geos.append(Geo(37.7936, -122.3930))
        geos.append(Geo(37.7831, -122.4181))
        geos.append(Geo(37.7863, -122.4015))

        for i in range(6, 100):
            lat: float = 37.7835 + i / 500.0 * 3
            lon: float = -122.41 + i / 500.0 * 4
            geos.append(Geo(lat, lon))

        rates = {}
        rates[1] = Rate(1, "RACK",
                        "2015-04-09",
                        "2015-04-10", 
                        { "BookableRate": 190.0,
                            "Code": "KNG",                          
                            "RoomDescription": "King sized bed",
                            "TotalRate": 109.0,
                            "TotalRateInclusive": 123.17})
        
        rates[2] = Rate(2, "RACK",
                        "2015-04-09",
                        "2015-04-10", 
                        { "BookableRate": 139.0,
                            "Code": "QN",                          
                            "RoomDescription": "Queen sized bed",
                            "TotalRate": 139.0,
                            "TotalRateInclusive": 153.09})
        
        rates[3] = Rate(3, "RACK",
                        "2015-04-09",
                        "2015-04-10", 
                        { "BookableRate": 109.0,
                            "Code": "KNG",                          
                            "RoomDescription": "King sized bed",
                            "TotalRate": 109.0,
                            "TotalRateInclusive": 123.17})
        
        for i in range(4, 80):
            if i % 3 == 0:
                hotel_id = i
                end_date = "2015-04-"
                rate = 109.0
                rate_inc = 123.17
                if i % 2 == 0:
                    end_date += '17'
                else:
                    end_date += '24'
                if i % 5 == 1:
                    rate = 120.0
                    rate_inc = 140.0
                elif i % 5 == 2:
                    rate = 124.0
                    rate_inc = 144.0
                elif i % 5 == 3:
                    rate = 132.0
                    rate_inc = 158.0
                elif i % 5 == 4:
                    rate = 232.0
                    rate_inc = 258.0
                
                rates[hotel_id] = Rate(i, "RACK",
                                        "2015-04-09",
                                        end_date, 
                                        { "BookableRate": rate,
                                            "Code": "KNG",                          
                                            "RoomDescription": "King sized bed",
                                            "TotalRate": rate,
                                            "TotalRateInclusive": rate_inc})
                
        # we don't create recommendations, because it doesn't really 
        # correspond to an entity
        prices = []

        prices.append(150.00)                           
        prices.append(120.00)                           
        prices.append(190.00)                           
        prices.append(160.00)                           
        prices.append(140.00)                           
        prices.append(200.00)                           

        for i in range(6, 100):
            price = 179.00
            if i % 3 == 0:
                if i % 5 == 0:
                    price = 123.17
                elif i % 5 == 1:
                    price = 140.00
                elif i % 5 == 2:
                    price = 144.00
                elif i % 5 == 3:
                    price = 158.00
                elif i % 5 == 4:
                    price = 258.00

            prices.append(price)

        # populate users
        self.users = [User(f"Cornell_{i}", str(i) * 10) for i in range(501)]
        for user in self.users:
            event = Event(self.init_user, {"user_id": user.id, "password": user.password}, None)
            self.runtime.send(event)

        # populate hotels
        self.hotels: list[Hotel] = []
        for i in range(100):
            geo = geos[i]
            rate = rates[i] if i in rates else []
            price = prices[i] 
            hotel = Hotel(str(i), 10, geo, rate, price)
            self.hotels.append(hotel)
            event = Event(self.init_hotel, 
                          {
                            "key": hotel.key,
                            "cap": hotel.cap,
                            "geo": hotel.geo,
                            "rates": hotel.rates,
                            "price": hotel.price
                            }, None)
            self.runtime.send(event)

        # populate flights
        self.flights = [Flight(str(i), 10) for i in range(100)]
        for flight in self.flights[:-1]:
            event = Event(self.init_flight, {
                "id": flight.id,
                "cap": flight.cap
            }, None)
            self.runtime.send(event)
        flight = self.flights[-1]
        event = Event(self.init_flight, {
                "id": flight.id,
                "cap": flight.cap
        }, None)
        self.runtime.send(event, flush=True)

def search_hotel():
    in_date = random.randint(9, 23)
    out_date = random.randint(in_date + 1, 24)

    if in_date < 10:
        in_date_str = f"2015-04-0{in_date}"
    else:
        in_date_str = f"2015-04-{in_date}"
    if out_date < 10:
        out_date_str = f"2015-04-0{out_date}"
    else:
        out_date_str = f"2015-04-{out_date}"

    lat = 38.0235 + (random.randint(0, 481) - 240.5) / 1000.0
    lon = -122.095 + (random.randint(0, 325) - 157.0) / 1000.0

    # We don't really use the in_date, out_date information
    return Event(search_op.dataflow.entry, {"lat": lat, "lon": lon}, search_op.dataflow)

def recommend(req_param=None):
    if req_param is None:
        coin = random.random()
        if coin < 0.5:
            req_param = "distance"
        else:
            req_param = "price"

    lat = 38.0235 + (random.randint(0, 481) - 240.5) / 1000.0
    lon = -122.095 + (random.randint(0, 325) - 157.0) / 1000.0


    return Event(recommend_op.dataflow.entry, {"requirement": req_param, "lat": lat, "lon": lon}, recommend_op.dataflow)

def user_login(succesfull=True):
    user_id = random.randint(0, 500)
    username = f"Cornell_{user_id}"
    password = str(user_id) * 10 if succesfull else ""
    return Event(OpNode(User, InvokeMethod("login"), read_key_from="user_key"), {"user_key": username, "password": password}, None)


def reserve():
    hotel_id = random.randint(0, 99)
    flight_id = random.randint(0, 99)
    
    user_id = "Cornell_" + str(random.randint(0, 500))

    return Event(
        user_op.dataflows["order"].entry, 
        {
            "user_key": user_id, 
            "flight_key": str(flight_id), 
            "hotel_key": str(hotel_id)
        }, 
        user_op.dataflows["order"])

def deathstar_workload_generator():
    search_ratio = 0.6
    recommend_ratio = 0.39
    user_ratio = 0.005
    reserve_ratio = 0.005
    c = 0
    while True:
        coin = random.random()
        if coin < search_ratio:
            yield search_hotel()
        elif coin < search_ratio + recommend_ratio:
            yield recommend()
        elif coin < search_ratio + recommend_ratio + user_ratio:
            yield user_login()
        else:
            yield reserve()
        c += 1

def reserve_workload_generator():
    while True:
        yield reserve()

def user_login_workload_generator():
    while True:
        yield user_login()

threads = 1
messages_per_burst = 1
sleeps_per_burst = 1
sleep_time = 0.0085
seconds_per_burst = 1
bursts = 100


def benchmark_runner(proc_num) -> dict[int, dict]:
    print(f'Generator: {proc_num} starting')
    client = FlinkClientSync("deathstar", "ds-out", "localhost:9092", True)
    deathstar_generator = reserve_workload_generator()
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
    ds = DeathstarDemo()
    ds.init_runtime(FlinkRuntime("deathstar", "ds-out"), bundle_time=5, bundle_size=10)
    ds.runtime.run(run_async=True)
    ds.populate()


    time.sleep(1)
    input()

    # with Pool(threads) as p:
    #     results = p.map(benchmark_runner, range(threads))

    # results = {k: v for d in results for k, v in d.items()}
    results = benchmark_runner(0)

    # pd.DataFrame({"request_id": list(results.keys()),
    #               "timestamp": [res["timestamp"] for res in results.values()],
    #               "op": [res["op"] for res in results.values()]
    #               }).sort_values("timestamp").to_csv(f'{SAVE_DIR}/client_requests.csv', index=False)
    print(results)
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