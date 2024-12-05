# todo: annotate with @cascade.entity
class User:
    def __init__(self, key: str, balance: int):
        self.key = key
        self.balance = balance

    def set_balance(self, balance: int):
        self.balance = balance
    
    def get_balance(self) -> int:
        return self.balance


def test_single_entity():
    user = User("user", 100)
    assert user.get_balance() == 100
    
    user.set_balance(10)
    assert user.get_balance() == 10