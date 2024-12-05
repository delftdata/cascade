# todo: annotate with @cascade.entity
class User:
    def __init__(self, key: str, balance: int):
        self.key = key
        self.balance = balance

    def buy_item(self, item: 'Item') -> bool:
        self.balance -= item.get_price()
        return self.balance >= 0


class Item:
    def __init__(self, key: str, price: int):
        self.key = key
        self.price = price
    
    def get_price(self):
        return self.price


def test_two_entities():
    user = User("user", 10)
    fork = Item("fork", 10)

    assert user.buy_item(fork) == True
    assert user.buy_item(fork) == False