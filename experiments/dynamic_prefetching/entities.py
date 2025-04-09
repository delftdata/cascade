from cascade import cascade
import random

@cascade 
class Oracle():
    @staticmethod
    def get() -> int:
        return 42

@cascade(globals={'random': random})
class Prefetcher:
    @staticmethod
    def prefetch(branch_chance: float):
        prefetched_value = Oracle.get()
        rand = random.random()
        cond = rand < branch_chance
        if cond:
            return prefetched_value
        else:
            return -42

    @staticmethod
    def baseline(branch_chance: float):
        cond = random.random() < branch_chance
        if cond:
            value = Oracle.get()
            return value
        else:
            return -42

