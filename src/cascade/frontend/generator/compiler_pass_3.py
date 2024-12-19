from cascade.frontend.generator.split_function import SplitFunction


class BuildCompiledMethodsString:

    def __init__(self, splits: list[SplitFunction]):
        self.splits: list[SplitFunction] = splits
    
    def make_splitfunctions(self) -> list[str]:
        bodies = []
        for split in self.splits:
            body = split.to_string()
            bodies.append(body)
        return '\n\n'.join(bodies)
    
    @classmethod
    def build(cls, splits: list[SplitFunction]):
        cls = cls(splits)
        return cls.make_splitfunctions()
