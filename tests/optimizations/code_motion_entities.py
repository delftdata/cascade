from cascade import cascade

@cascade
class Item:
    def __init__(self, item: str, quantity: int, price: int):
        self.item = item
        self.quantity = quantity
        self.price = price

    def get_quantity(self):
        return self.quantity
    
    def get_price(self):
        return self.price



@cascade
class User:
    def __init__(self, balance: int):
        self.balance = balance

    def checkout_item(self, item: Item):
        stock = item.get_quantity()
        in_stock = stock > 0
        price = item.get_price()
        can_buy = price <= self.balance
        condition = in_stock and can_buy
        if condition:
            self.balance = self.balance - price
            return True
        else:
            return False
        
    def get_balance(self) -> int:
        return self.balance