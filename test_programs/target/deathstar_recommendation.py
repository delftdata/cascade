from typing import Literal
import cascade

# Stateless
@cascade.cascade
class Recommendation():
    @staticmethod
    def get_recommendations(requirement: Literal["distance", "price"], lat: float, lon: float) -> list[Hotel]:
        if requirement == "distance":
            distances = [(hotel.geo.distance_km(lat, lon), hotel)
                            for hotel in Hotel.__all__()]
            min_dist = min(distances, key=lambda x: x[0])
            res = [hotel for dist, hotel in distances if dist == min_dist]
        elif requirement == "price":
            prices = [(hotel.price, hotel)
                            for hotel in Hotel.__all__()]
            min_price = min(prices, key=lambda x: x[0])
            res = [hotel for rate, hotel in prices if rate == min_price]

        return res