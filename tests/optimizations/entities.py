from cascade import cascade

@cascade
class Stock:
    def __init__(self, item: str, quantity: int):
        self.item = item
        self.quantity = quantity

    def get_quantity(self):
        return self.quantity

@cascade
class Adder:
    @staticmethod
    def add(a, b):
        return a + b

@cascade
class Test:
    @staticmethod
    def get_total(item1: Stock, item2: Stock):
        x = item1.get_quantity()
        y = item2.get_quantity()
        total_adder = Adder.add(x, y)
        return total_adder