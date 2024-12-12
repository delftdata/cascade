from klara.core.ssa_visitors import AstVisitor
from klara.core.nodes import ClassDef, Module
from klara.core.cfg import Cfg, RawBasicBlock

class ExtractEntityVisitor(AstVisitor):

    def __init__(self):
        self.entities: dict[str, int] = {}
        self.color: int = 1

    def visit_classdef(self, node: ClassDef):
        name: str = str(node.name)
        if name not in self.entities:
            self.entities[name] = self.color
            self.color += 1
    
    def visit_cfg(self, cfg: Cfg):
        for block in cfg.block_list:
            block: RawBasicBlock
            for node in block.ssa_code:
                if type(node) in [Module]:
                    continue
                self.visit(node)
    
    def get_entity_map(self):
        return self.entities

    @classmethod
    def extract(cls, node):
        c = cls()
        if type(node) == Cfg:
            c.visit_cfg(node)
        else:
            c.visit(node)
        return c.get_entity_map()
