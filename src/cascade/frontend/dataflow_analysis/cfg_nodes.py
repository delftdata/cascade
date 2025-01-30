from dataclasses import dataclass
import abc

import ast


class BaseBlock:

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
class _BaseBlockDefaults:
    _name: str = None

    @property
    def name(self):
        assert self._name, 'Name is not set'
        return self._name
    
    @name.setter
    def name(self, name: str):
        """ Split function builder sets method name such that dataflow builder can use this name.
        """
        assert self._name == None, f'Name shouldbe only set once. new name: {name} old name: {self._name}'
        self._name = name


@dataclass
class _Block(BaseBlock):
    statements: list[ast.stmt]
    next_block: BaseBlock = None
    
    def set_next_block(self, block: BaseBlock):
        self.next_block = block
    
    def get_next_blocks(self):
        return [self.next_block]
    
    def replace_link(self, new: BaseBlock, old: BaseBlock):
        assert self.next_block == old, f'Old block ({old}) is expected to be equal to next_block ({self.next_block}) on replacement.'
        self.next_block = new


@dataclass
class _SplitBlock(BaseBlock):
    statements: list[ast.stmt]
    remote_function_calls: list[ast.stmt]


@dataclass
class _SplitBlockDefaults(_BaseBlockDefaults):
    next_block: BaseBlock = None
    
    def set_next_block(self, block):
        self.next_block = block

    def get_next_blocks(self):
        return [self.next_block]


@dataclass
class _IFBlock(BaseBlock):
    test: ast.stmt
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


@dataclass
class _ForLoop(BaseBlock):
    loop_condition: ast.stmt
    body: list[ast.stmt] 


@dataclass
class Block(_BaseBlockDefaults, _Block):
    pass 


@dataclass
class IFBlock(_BaseBlockDefaults, _IFBlock):
    pass


@dataclass
class SplitBlock(_SplitBlockDefaults, _SplitBlock):
    pass


@dataclass
class ForLoop(_BaseBlockDefaults, BaseBlock):
    pass
