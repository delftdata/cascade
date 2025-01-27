import ast 
import sys

from klara.core.ssa_visitors import AstVisitor
from klara.core import nodes
from klara.core.tree_rewriter import AstBuilder
from klara.core.cfg import Cfg
from klara.core.tree_rewriter import TreeRewriter
from klara.core.node_classes import BUILT_IN_TYPE, BUILT_IN_TYPE_MAP


class SSAConverter(AstVisitor):
    
    def __init__(self, ast_module: ast.Module):
        self.as_tree = TreeRewriter().visit_module(ast_module, name="")
    
    def convert(self):
        cfg = Cfg(self.as_tree)
        cfg.convert_to_ssa()
        return self.visit(cfg.as_tree)
    
    def fields_to_attr_map(self, node):
        fields = node._fields
        return {v: self.generic_visit(getattr(node, v)) for v in fields}

    def visit(self, node):
        match type(node):
            case nodes.Alias:
                class_object = ast.alias
            case nodes.Arguments:
                class_object = ast.arguments
            case nodes.Arg:
                class_object = ast.arg
            case nodes.Const:
                class_object = ast.Constant
            case nodes.AssignName:
                attrs = self.fields_to_attr_map(node)
                id_: str = repr(node)
                return ast.Name(id_, ctx=attrs['ctx'])
            case nodes.Store:
                class_object = ast.Store
            case nodes.Bool:
                class_object = ast.BoolOp
            case nodes.AssignAttribute:
                class_object = ast.Assign
            case nodes.Name:        
                attrs = self.fields_to_attr_map(node)
                id_: str = repr(node)
                return ast.Name(id_, ctx=attrs['ctx'])
            case _:
                class_name: str = node.__class__.__name__
                if class_name in BUILT_IN_TYPE_MAP:
                    return node
                if class_name == 'type':
                    class_name = node.__name__
                class_object = self.get_ast_class(class_name)
        attrs = self.fields_to_attr_map(node)
        obj = class_object(**attrs)
        return obj
    
    def generic_visit(self, node):
        if node == None:
            return None
        elif isinstance(node, list):
            return [self.generic_visit(item) for item in node]
        else:
            return self.visit(node)


    @staticmethod
    def get_ast_class(class_name: str):
        return getattr(sys.modules[ast.__name__], class_name)


 
