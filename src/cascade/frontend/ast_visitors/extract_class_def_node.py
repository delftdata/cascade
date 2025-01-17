from klara.core.ssa_visitors import AstVisitor
from klara.core import nodes

class ExtractClassDefNode(AstVisitor):


    def __init__(self, target_class_name: str):
        self.target_class_name: str = target_class_name
        self.class_def: nodes.ClassDef = None
    
    def visit_classdef(self, node: nodes.ClassDef):
        if node.name == self.target_class_name:
            self.class_def = node
    
    @classmethod
    def extract(cls, node: nodes.Module, target_class_name: str) -> nodes.ClassDef:
        c = cls(target_class_name)
        c.visit(node)
        if c.class_def == None:
            raise Exception(f'Class with name {target_class_name} does not exsist in Module: {node.name}')
        return c.class_def
