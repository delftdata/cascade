from typing import Any
from cascade.dataflow.dataflow import Operator
from cascade.dataflow.operator import StatefulOperator


class Flight():
    def __init__(self, id: str, cap: int):
        self.id = id
        self.cap = cap
        # self.customers = []

    # In order to be deterministic, we don't actually change the capacity
    def reserve(self) -> bool:
        if self.cap <= 0:
            return False
        return True


#### COMPILED FUNCTIONS (ORACLE) #####

def reserve_compiled(variable_map: dict[str, Any], state: Flight, key_stack: list[str]) -> Any:
    key_stack.pop()
    if state.cap <= 0:
        return False
    return True

flight_op = StatefulOperator(
    Flight,
    {
        "reserve": reserve_compiled
    },
    {} # no dataflow?
)
