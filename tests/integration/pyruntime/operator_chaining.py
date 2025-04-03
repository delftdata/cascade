import cascade

@cascade.cascade
class C:
    def __init__(self, key: str):
        self.key = key

    def get(self, y: int) -> int:
        test = 42 + y
        return test
    
    def __key__(self) -> str:
        return self.key

@cascade.cascade
class B:
    def __init__(self, key: str):
        self.key = key

    def call_c(self, c: C) -> int:
        y = 0 
        x = c.get(y)
        return x
    
    def __key__(self) -> str:
        return self.key
    
@cascade.cascade
class A:
    def __init__(self, key: str):
        self.key = key

    def call_c_thru_b(self, b: B, c: C) -> int:
        x = b.call_c(c)
        return x*2
    
    def __key__(self) -> str:
        return self.key

     