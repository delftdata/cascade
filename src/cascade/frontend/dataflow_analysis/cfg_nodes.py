from dataclasses import dataclass
import abc

from klara.core.nodes import Statement


@dataclass
class BaseBlock:
    pass

    @abc.abstractmethod
    def set_next_block(self, block: "BaseBlock"):
        pass

    @abc.abstractmethod
    def get_next_blocks(self):
        pass

    @abc.abstractmethod
    def replace_link(self, new: "BaseBlock", old: "BaseBlock"):
        return


@dataclass
class Block(BaseBlock):
    statements: list[Statement]
    next_block: BaseBlock = None
    
    def set_next_block(self, block: BaseBlock):
        self.next_block = block
    
    def get_next_blocks(self):
        return [self.next_block]
    
    def replace_link(self, new: BaseBlock, old: BaseBlock):
        assert self.next_block == old, f'Old block ({old}) is expected to be equal to next_block ({self.next_block}) on replacement.'
        self.next_block = new


@dataclass
class SplitBlock(BaseBlock):
    statements: list[Statement]
    remote_function_calls: list[Statement]
    next_block: BaseBlock = None

    def set_next_block(self, block):
        self.next_block = block

    def get_next_blocks(self):
        return [self.next_block]


@dataclass
class IFBlock(BaseBlock):
    test: Statement
    body: BaseBlock
    or_else: BaseBlock

    def set_next_block(self, block):
        for b in [self.body, self.or_else]:
            b.set_next_block(block)

    def get_next_blocks(self):
        return [self.body, self.or_else]
    
    def replace_link(self, new: BaseBlock, old: BaseBlock):
        if self.body == old:
            self.body = new
        elif self.or_else == old:
            self.or_else == new
        else:
            assert False, f'Both body ({self.body}) and or_else ({self.or_else}) did not equal old ({self.old}) block'
        

class ForLoop(BaseBlock):
    loop_condition: Statement
    body: list[Statement] 
