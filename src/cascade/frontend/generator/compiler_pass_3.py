
from cascade.frontend.intermediate_representation import Statement
from cascade.frontend.generator.split_function import SplitFunction

from klara.core import nodes

from klara.core.cfg import RawBasicBlock

class CompilerPass3:

    def __init__(self, splits: list[list[Statement]]):
        self.splits: list[list[SplitFunction]] = splits
    
    def make_splitfunctions(self) -> list[str]:
        bodies = [] # (OpNode, SplitFunction (list[str])
        for split in self.splits:
            body = split.to_string()
            bodies.append(body)
        return bodies


    # def make_dataflow(self) -> DataFlow:
        
    #     first, *rest = self.splits
    #     entry: Statement = first.pop(0)
    #     in_vars = entry.targets
    #     pass
 