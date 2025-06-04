import cascade

@cascade.cascade
class User():
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

    def login(self, password: str) -> bool:
        return self.password == password
    
