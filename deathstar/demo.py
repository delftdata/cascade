import random
import sys
import os
import time
from confluent_kafka import Producer
import pickle
from timeit import default_timer as timer
from multiprocessing import Pool

# import cascade
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from cascade.dataflow.dataflow import Event, InitClass, InvokeMethod, OpNode
from cascade.runtime.flink_runtime import FlinkOperator, FlinkRuntime, FlinkStatelessOperator
from deathstar.entities.flight import Flight, flight_op
from deathstar.entities.hotel import Geo, Hotel, Rate, hotel_op
from deathstar.entities.recommendation import Recommendation, recommend_op
from deathstar.entities.search import Search, search_op
from deathstar.entities.user import User, user_op


class DeathStarDemo():
    def __init__(self, name):
        self.init_user = OpNode(user_op, InitClass())
        self.init_hotel = OpNode(hotel_op, InitClass())
        self.init_flight = OpNode(flight_op, InitClass())
        self.runtime = FlinkRuntime(name)
    
    def init_runtime(self):
        self.runtime.init(bundle_time=100, bundle_size=1000)
        self.runtime.add_operator(FlinkOperator(hotel_op))
        self.runtime.add_operator(FlinkOperator(flight_op))
        self.runtime.add_operator(FlinkOperator(user_op))
        self.runtime.add_stateless_operator(FlinkStatelessOperator(search_op))
        self.runtime.add_stateless_operator(FlinkStatelessOperator(recommend_op))


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
            event = Event(self.init_user, [user.id], {"user_id": user.id, "password": user.password}, None)
            self.runtime.send(event)

        # populate hotels
        self.hotels: list[Hotel] = []
        for i in range(100):
            geo = geos[i]
            rate = rates[i] if i in rates else []
            price = prices[i] 
            hotel = Hotel(str(i), 10, geo, rate, price)
            self.hotels.append(hotel)
            event = Event(self.init_hotel, [hotel.key], 
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
            event = Event(self.init_flight, [flight.id], {
                "id": flight.id,
                "cap": flight.cap
            }, None)
            self.runtime.send(event)
        flight = self.flights[-1]
        event = Event(self.init_flight, [flight.id], {
                "id": flight.id,
                "cap": flight.cap
        }, None)
        self.runtime.send(event, flush=True)

class Client:
    def __init__(self, topic="input-topic", kafka_broker="localhost:9092"):
        self.producer = Producer({'bootstrap.servers': kafka_broker})
        self.topic = topic

    def send(self, event: Event, flush=False):
        """Send an event to the Kafka source.
        Once `run` has been called, the Flink runtime will start ingesting these 
        messages. Messages can always be sent after `init` is called - Flink 
        will continue ingesting messages after `run` is called asynchronously.
        """
        self.producer.produce(self.topic, value=pickle.dumps(event))
        if flush:
            self.producer.flush()

    def search_hotel(self):
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
        event = Event(search_op.dataflow.entry, ["tempkey"], {"lat": lat, "lon": lon}, search_op.dataflow)
        self.send(event)

    def recommend(self, req_param=None):
        if req_param is None:
            coin = random.random()
            if coin < 0.5:
                req_param = "distance"
            else:
                req_param = "price"

        lat = 38.0235 + (random.randint(0, 481) - 240.5) / 1000.0
        lon = -122.095 + (random.randint(0, 325) - 157.0) / 1000.0

        event = Event(recommend_op.dataflow.entry, ["tempkey"], {"requirement": req_param, "lat": lat, "lon": lon}, recommend_op.dataflow)
        self.send(event)

    def user_login(self):
        user_id = random.randint(0, 500)
        username = f"Cornell_{user_id}"
        password = str(user_id) * 10
        event = Event(OpNode(user_op, InvokeMethod("login")), [username], {"password": password}, None)
        self.send(event)

    def reserve(self):
        hotel_id = random.randint(0, 99)
        flight_id = random.randint(0, 99)
        
        # user = User("user1", "pass")
        # user.order(flight, hotel)
        user_id = "Cornell_" + str(random.randint(0, 500))

        event = Event(user_op.dataflows["order"].entry, [user_id], {"flight": str(flight_id), "hotel": str(hotel_id)}, user_op.dataflows["order"])
        self.send(event)

    def deathstar_workload_generator(self):
        search_ratio = 0.6
        recommend_ratio = 0.39
        user_ratio = 0.005
        reserve_ratio = 0.005
        c = 0
        while True:
            coin = random.random()
            if coin < search_ratio:
                yield self.search_hotel()
            elif coin < search_ratio + recommend_ratio:
                yield self.recommend()
            elif coin < search_ratio + recommend_ratio + user_ratio:
                yield self.user_login()
            else:
                yield self.reserve()
            c += 1
    
threads = 3
messages_per_second = 100
sleeps_per_second = 100
sleep_time = 0.0085
seconds = 50

def benchmark_runner(proc_num) -> dict[bytes, dict]:
    print(f'Generator: {proc_num} starting')
    client = Client("deathstar")
    deathstar_generator = client.deathstar_workload_generator()
    timestamp_futures: dict[bytes, dict] = {}
    start = timer()
    for _ in range(seconds):
        sec_start = timer()
        for i in range(messages_per_second):
            if i % (messages_per_second // sleeps_per_second) == 0:
                time.sleep(sleep_time)
            # operator, key, func_name, params = next(deathstar_generator)
            # future = client.send_event(operator=operator,
            #                          key=key,
            #                          function=func_name,
            #                          params=params)
            # timestamp_futures[future.request_id] = {"op": f'{func_name} {key}->{params}'}
            next(deathstar_generator)
        # styx.flush()
        sec_end = timer()
        lps = sec_end - sec_start
        if lps < 1:
            time.sleep(1 - lps)
        sec_end2 = timer()
        print(f'Latency per second: {sec_end2 - sec_start}')
    end = timer()
    print(f'Average latency per second: {(end - start) / seconds}')
    # styx.close()
    # for key, metadata in styx.delivery_timestamps.items():
    #     timestamp_futures[key]["timestamp"] = metadata
    return timestamp_futures


def main():

    ds = DeathStarDemo("deathstar")
    ds.init_runtime()
    ds.runtime.run(run_async=True)
    ds.populate()


    time.sleep(1)
    input()

    with Pool(threads) as p:
        results = p.map(benchmark_runner, range(threads))

    results = {k: v for d in results for k, v in d.items()}

    # pd.DataFrame({"request_id": list(results.keys()),
    #               "timestamp": [res["timestamp"] for res in results.values()],
    #               "op": [res["op"] for res in results.values()]
    #               }).sort_values("timestamp").to_csv(f'{SAVE_DIR}/client_requests.csv', index=False)
    print(results)

if __name__ == "__main__":
    main()