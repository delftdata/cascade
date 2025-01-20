from dataclasses import dataclass
import abc

from klara.core.nodes import Statement


@dataclass
class BaseBlock:
    pass

    @abc.abstractmethod
    def set_next_block(self, block: "BaseBlock"):
        pass


@dataclass
class Block(BaseBlock):
    statements: list[Statement]
    next_block: BaseBlock = None
    
    def set_next_block(self, block: BaseBlock):
        self.next_block = block


@dataclass
class IFBlock(BaseBlock):
    test: Statement
    body: BaseBlock
    or_else: BaseBlock

    def set_next_block(self, block):
        for b in [self.body, self.or_else]:
            b.set_next_block(block)

class ForLoop(BaseBlock):
    loop_condition: Statement
    body: list[Statement] 
