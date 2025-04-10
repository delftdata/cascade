from cascade import cascade
import random
import time

@cascade(globals={'time': time})
class Oracle():
    @staticmethod
    def get() -> int:
        time.sleep(0.01)
        return 42

@cascade(globals={'random': random})
class Prefetcher:
    @staticmethod
    def prefetch(branch_chance: float):
        prefetched_value = Oracle.get()
        and_also = Oracle.get()
        rand = random.random()
        cond = rand < branch_chance
        if cond:
            return prefetched_value
        else:
            return -42

    @staticmethod
    def baseline(branch_chance: float):
        and_also = Oracle.get()
        cond = random.random() < branch_chance
        if cond:
            value = Oracle.get()
            return value
        else:
            return -42

