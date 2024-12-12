from dataclasses import dataclass, field
from klara.core.cfg import RawBasicBlock

from cascade.frontend.intermediate_representation import Block

@dataclass
class Statement:
    block_num: int
    block: RawBasicBlock
    targets: list[str] = field(default_factory=list)
    values: list[str] = field(default_factory=list)
    remote_call: bool = False
    attribute_name: str = None
    parent_block: Block = None
    color: int = 0

    def extend_targets(self, new_targets: list[str]):
        self.targets.extend(new_targets)
    
    def extend_values(self, new_values: list[str]):
        self.values.extend(new_values)
    
    def set_remote(self):
        self.remote_call = True
    
    def set_attribute_name(self, name: str):
        self.attribute_name = name
    
    def set_color(self, color: int):
        self.color = color
    
    def set_parent_block(self, parent: Block):
        self.parent_block = parent
    
    def get_parent_block(self):
        return self.parent_block
    
    def __hash__(self):
        return hash(self.block_num)
