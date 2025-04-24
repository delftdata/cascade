import cascade

@cascade.cascade
class User:
    def __init__(self, username: str, balance: int):
        self.username = username
        self.balance = balance
    
    def buy_item(self, item: 'Item') -> bool:
        item_price = item.get_price() # SSA
        self.balance = self.balance - item_price
        return self.balance >= 0
    
    def __key__(self) -> str:
        return self.username

@cascade.cascade
class Item:
    def __init__(self, item_name: str, price: int):
        self.item_name = item_name
        self.price = price

    def get_price(self) -> int:
        return self.price
    
    def __key__(self) -> str:
        return self.item_name
     