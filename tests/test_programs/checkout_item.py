class User:
    def __init__(self, key: str, balance: int):
        self.key: str = key
        self.balance: int = balance
    
    def buy_item(self, item: 'Item') -> bool:
        item_price = item.get_price() # SSA
        self.balance -= item_price
        return self.balance >= 0

class Item:
    def __init__(self, key: str, price: int):
        self.key: str = key
        self.price: int = price

    def get_price(self) -> int:
        return self.price