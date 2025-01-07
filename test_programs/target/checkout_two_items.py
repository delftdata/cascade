import cascade

@cascade.cascade
class User:
    def __init__(self, key: str, balance: int):
        self.key: str = key
        self.balance: int = balance
    
    def buy_two_items(self, item_1: 'Item', item_2: 'Item') -> bool:
        item_price_1 = item_1.get_price()
        item_price_2 = item_2.get_price() 
        total_price = item_price_1 + item_price_2
        self.balance -= total_price
        return self.balance >= 0

@cascade.cascade
class Item:
    def __init__(self, key: str, price: int):
        self.key: str = key
        self.price: int = price

    def get_price(self) -> int:
        return self.price