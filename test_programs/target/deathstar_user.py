import cascade

@cascade.cascade
class User():
    def __init__(self, user_id: str, password: str):
        self.id = user_id
        self.password = password

    def check(self, password):
        return self.password == password
    
    def order(self, flight: Flight, hotel: Hotel):
        if hotel.reserve() and flight.reserve():
            return True
        else:
            return False