from dataclasses import dataclass, field
from klara.core.cfg import RawBasicBlock

from klara.core.nodes import Attribute, Return

@dataclass
class Statement:
    block_num: int
    block: RawBasicBlock
    targets: list[str] = field(default_factory=list)
    values: list[str] = field(default_factory=list)
    remote_call: bool = False
    is_predicate: bool = False
    attribute: Attribute = None

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
    
    def is_remote(self) -> bool:
        return self.remote_call
    
    def is_return(self) -> bool:
        return isinstance(self.block, Return)
    
    def __hash__(self):
        return hash(self.block_num)
