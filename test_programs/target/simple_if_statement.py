import cascade

@cascade.cascade
class User:

    def buy_item(self, item: 'Item', delivery_service: 'DeliveryService') -> bool:
        item_price = item.get_price()
        if item_price < 100:
            delivery_costs = delivery_service.get_delivery_costs(item_price)
            total_price = item_price + delivery_costs
        else:
            total_price = item_price
        self.balance -= total_price
        return self.balance >= 0