import cascade

@cascade.cascade
class Brancher:
    @staticmethod
    def branch(cond: bool) -> int:
        x = 10
        if cond:
            r = Remote.get()
            return r
        else:
            return 42
        
    @staticmethod
    def branch_insta(cond: bool) -> int:
        if cond:
            r = Remote.get()
            return r
        else:
            return 42
        

@cascade.cascade
class Remote:
    @staticmethod
    def get() -> int:
        return 33
        
        


     