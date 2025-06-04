from cascade import cascade
import random
import time

@cascade(globals={'time': time})
class Oracle():
    @staticmethod
    def get() -> int:
        time.sleep(0.01)
        return 42

@cascade(globals={'random': random})
class Prefetcher:
    @staticmethod
    def prefetch(branch_chance: float):
        prefetched_value = Oracle.get()
        and_also = Oracle.get()
        rand = random.random()
        cond = rand < branch_chance
        if cond:
            return prefetched_value
        else:
            return -42

    @staticmethod
    def baseline(branch_chance: float):
        and_also = Oracle.get()
        cond = random.random() < branch_chance
        if cond:
            value = Oracle.get()
            return value
        else:
            return -42

@cascade
class User():
    def __init__(self, key: str, is_premium: bool, preferences: list[str]):
        self.key = key
        self.is_premium = is_premium
        self.preferences = preferences

    def is_ad_free(self) -> bool:
        return self.is_premium
    
    def get_preferences(self) -> list[str]:
        return self.preferences

@cascade
class NavigationService:
    @staticmethod
    def get_directions_baseline(origin: int, dest: int, user: User):
        directions = MapService.get_route(origin, dest)
        cond = not user.is_ad_free()
        if cond:
            recc = Recommender.get_recommendations(dest, user)
            return (directions, recc)
        else:
            return (directions, None)
    
    @staticmethod
    def get_directions_prefetch(origin: int, dest: int, user: User):
        directions = MapService.get_route(origin, dest)
        recc = Recommender.get_recommendations(dest, user)
        cond = not user.is_ad_free()
        if cond:
            return (directions, recc)
        else:
            return (directions, None)

@cascade(globals={'time': time})    
class MapService:
    @staticmethod
    def get_route(origin: int, dest: int):
        time.sleep(0.01)
        return "left right left"

@cascade    
class Recommender:
    @staticmethod
    def get_recommendations(dest: int, user: User) -> str:
        user_preferences = user.get_preferences()
        recs = []

        cond = "restaurants" in user_preferences
        if cond:
            recs_0.extend(["McDonalds", "Starbucks"])
        else:
            pass

        # cond = "museums" in user_preferences
        # if cond:
        #     recs_0.extend(["Louvre"])
        # else:
        #     pass

        cond = "attractions" in user_preferences
        if cond:
            recs_0.extend(["Eiffel Tower", "Arc de Triomphe"])
        else:
            pass

        return ", ".join(recs)