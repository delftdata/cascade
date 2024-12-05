class User:
    def __init__(self, key: str, balance: int):
        self.key = key
        self.balance = balance

    def buy_items(self, items: list['Item']) -> bool:
        total_price = sum([item.get_price() for item in items])
        self.balance -= total_price
        return self.balance >= 0


class Item:
    def __init__(self, key: str, price: int):
        self.key = key
        self.price = price
    
    def get_price(self):
        return self.price
    

def test_merge_operator():
    user1 = User("user1", 100)
    user2 = User("user2", 80)

    fork = Item("fork", 20)
    knife = Item("knife", 20)
    plate = Item("plate", 50)
    assert user1.buy_items([fork, knife, plate]) == True
    assert user2.buy_items([fork, knife, plate]) == False