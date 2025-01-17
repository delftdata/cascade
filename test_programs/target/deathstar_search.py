import cascade

# Stateless
@cascade.cascade
class Search():
    # Get the 5 nearest hotels
    @staticmethod
    def nearby(lat: float, lon: float, in_date: int, out_date: int):
        distances = [
            (dist, hotel) 
                    for hotel in Hotel.__all__() 
                    if (dist := hotel.geo.distance_km(lat, lon)) < 10]
        hotels = [hotel for dist, hotel in sorted(distances)[:5]]
        return hotels