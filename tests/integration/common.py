import cascade

@cascade.cascade
class User:
    def __init__(self, key: str, balance: int):
        self.key: str = key
        self.balance: int = balance

    def update_balance(self, amount: int) -> bool:
        self.balance = self.balance + amount
        return self.balance >= 0
    
    def get_balance(self) -> int:
        return self.balance
    
    def buy_item(self, item: 'Item') -> bool:
        item_price = item.get_price() # SSA
        self.balance = self.balance - item_price
        return self.balance >= 0
    
    def buy_2_items(self, item1: 'Item', item2: 'Item') -> bool:
        item1_price = item1.get_price() # SSA
        item2_price = item2.get_price() # SSA
        total = item1_price + item2_price
        self.balance = self.balance - total
        return self.balance >= 0
    
   
@cascade.cascade
class Item:
    def __init__(self, key: str, price: int):
        self.key: str = key
        self.price: int = price

    def get_price(self) -> int:
        return self.price