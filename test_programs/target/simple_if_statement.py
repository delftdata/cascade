import cascade


@cascade.cascade
class User:

    def buy_item(self, item: 'Item', delivery_service: 'DeliveryService') -> bool:
        item_price = item.get_price()
        if item_price < 100:
            delivery_costs = delivery_service.get_delivery_costs()
            total_price = item_price + delivery_costs
        else:
            total_price = item_price
        self.balance -= total_price
        return self.balance >= 0


@cascade.cascade
class Item:
    
    def __init__(self, item_price):
        self.item_price: int = item_price

    def get_price(self):
        return self.item_price


class DeliveryService:

    def get_delivery_costs(self):
        return 4.88