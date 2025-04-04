import cascade

@cascade.cascade
class User:
    def __init__(self, username: str, balance: int):
        self.username = username
        self.balance = balance
    
    def buy_item_easy(self, item: 'Item') -> int:
        item_price = item.get_price()
        cond = self.balance - item_price >= 0
        if cond:
            self.balance = self.balance - item_price
        else:
            x = 10
        return self.balance
    
    # def buy_item_pred(self, item: 'Item') -> int:
    #     item_price = item.get_price()
    #     if self.balance - item_price >= 0:
    #         self.balance = self.balance - item_price
    #     return self.balance
    
    # def buy_item_else(self, item: 'Item') -> str:
    #     item_price = item.get_price()
    #     if self.balance - item_price >= 0:
    #         item_price = item.get_price()
    #         self.balance = self.balance - item_price
    #         return "item bought"
    #     else:
    #         item_price = item.get_price()
    #         msg = str(item_price) + " is too expensive!"
    #         return msg
    
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
