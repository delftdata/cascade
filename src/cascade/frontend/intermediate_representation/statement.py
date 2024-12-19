from dataclasses import dataclass, field
from klara.core.cfg import RawBasicBlock

from klara.core.nodes import Attribute

@dataclass
class Statement:
    block_num: int
    block: RawBasicBlock
    targets: list[str] = field(default_factory=list)
    values: list[str] = field(default_factory=list)
    remote_call: bool = False
    attribute: Attribute = None
    color: int = 0

    def extend_targets(self, new_targets: list[str]):
        self.targets.extend(new_targets)
    
    def extend_values(self, new_values: list[str]):
        self.values.extend(new_values)
    
    def set_remote(self):
        self.remote_call = True
    
    @property
    def attribute_name(self):
        return repr(self.attribute.value)
    
    def set_attribute(self, attribute: Attribute):
        self.attribute = attribute
    
    def set_color(self, color: int):
        self.color = color
    
    def is_remote(self) -> bool:
        return self.remote_call
    
    def __hash__(self):
        return hash(self.block_num)
