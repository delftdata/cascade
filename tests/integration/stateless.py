import cascade


@cascade.cascade
class SomeStatelessOp:
    @staticmethod
    def get() -> int:
        return 42