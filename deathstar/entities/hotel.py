from dataclasses import dataclass
from typing import Any, Optional
from cascade.dataflow.operator import StatefulOperator
from geopy.distance import distance


@dataclass
class Geo():
    lat: float
    lon: float
    
    def distance_km(self, lat: float, lon: float):
        return distance((lat, lon), (self.lat, self.lon)).km

@dataclass
class Rate():
    key: int
    code: str
    in_date: str
    out_date: str
    room_type: dict

    def __key__(self):
        return self.key
        
# todo: add a linked entity
# e.g. reviews: list[Review] where Review is an entity
class Hotel():
    def __init__(self, 
                 key: str,
                 cap: int, 
                 geo: Geo,
                 rates: list[Rate], 
                 price: float):
        self.key = key
        self.cap = cap
        self.customers = []
        self.rates = rates
        self.geo = geo
        self.price = price

    # In order to be deterministic, we don't actually change the capacity
    def reserve(self) -> bool:
        if self.cap < 0:
            return False
        return True

    def get_geo(self) -> Geo:
        return self.geo

    @staticmethod
    def __all__() -> list['Hotel']:
        pass

    def __key__(self) -> int:
        return self.key
    


#### COMPILED FUNCTIONS (ORACLE) #####

def reserve_compiled(variable_map: dict[str, Any], state: Hotel, key_stack: list[str]) -> Any:
    key_stack.pop()
    if state.cap <= 0:
        return False
    return True

def get_geo_compiled(variable_map: dict[str, Any], state: Hotel, key_stack: list[str]) -> Any:
    key_stack.pop()
    return state.geo

def get_price_compiled(variable_map: dict[str, Any], state: Hotel, key_stack: list[str]) -> Any:
    key_stack.pop()
    return state.price

hotel_op = StatefulOperator(
    Hotel,
    {
        "reserve": reserve_compiled,
        "get_geo": get_geo_compiled,
        "get_price": get_price_compiled
    },
    {} # no dataflow?
)
