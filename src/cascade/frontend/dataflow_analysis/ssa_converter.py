import ast
import sys

from cascade.frontend.ast_ import SSAName



from klara.core.ssa_visitors import AstVisitor
from klara.core import nodes
from klara.core.tree_rewriter import AstBuilder
from klara.core.cfg import Cfg
from klara.core.tree_rewriter import TreeRewriter
from klara.core.node_classes import BUILT_IN_TYPE, BUILT_IN_TYPE_MAP


bin_op_from_str = {
    "+": ast.Add,
    "&": ast.BitAnd,
    "|": ast.BitOr,
    "^": ast.BitXor,
    "/": ast.Div,
    "//": ast.FloorDiv,
    "*": ast.Mult,
    "**": ast.Pow,
    "-": ast.Sub,
    "<<": ast.LShift,
    ">>": ast.RShift,
    "%": ast.Mod
    }


comp_op_from_str = { 
    "<": ast.Lt,
    ">": ast.Gt,
    "<=": ast.LtE,
    ">=": ast.GtE,
    "==": ast.Eq,
    "!=": ast.NotEq,
    "is": ast.Is,
    "is not": ast.IsNot,
    "in": ast.In,
    "not in": ast.NotIn
    }

context_from_module = {nodes.Load: ast.Load, nodes.Store: ast.Store, nodes.Del: ast.Del}

unary_op_from_str = {"+": ast.UAdd, "-": ast.USub, "not": ast.Not, "~": ast.Invert}

bool_op_from_str = {"and": ast.And, "or": ast.Or}

class SSAConverter(AstVisitor):
    
    def __init__(self, f_def: ast.FunctionDef):
        m = ast.Module([f_def])
        self.as_tree = TreeRewriter().visit_module(m)
    
    def convert(self):
        cfg = Cfg(self.as_tree)
        cfg.convert_to_ssa()
        m = self.visit(cfg.as_tree)
        f_def, = m.body
        return f_def
    
    def fields_to_attr_map(self, node):
        fields = node._fields
        return {v: self.generic_visit(getattr(node, v)) for v in fields}

    def visit(self, node) -> ast.AST:
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
                return ast.Name(node.id, version=node.version, ctx=attrs['ctx'])
            case nodes.Load:
                return ast.Load()
            case nodes.Store:
                return ast.Store()
            case nodes.Del:
                return ast.Del()
            case nodes.Bool:
                # in klara a Bool node wraps any logical expression. The Bool needs to be unpacked.
                return self.visit(node.value)
            case nodes.AssignAttribute:
                #TODO: fix this one not having targets
                return ast.Attribute(value=self.visit(node.value), 
                                                        attr=node.attr, 
                                                        ctx=context_from_module[node.ctx]())
            case nodes.Name:
                attrs = self.fields_to_attr_map(node)
                return SSAName(node.id, version=node.version, ctx=attrs['ctx'])
            case nodes.Compare:
                return ast.Compare(self.visit(node.left), [comp_op_from_str[o]() for o in node.ops], [self.visit(c) for c in node.comparators])
            case nodes.BinOp:
                return ast.BinOp(self.visit(node.left), bin_op_from_str[node.op](), self.visit(node.right))
            case nodes.BoolOp:
                return ast.BoolOp(bool_op_from_str[node.op](), [self.visit(v) for v in node.values])
            case nodes.UnaryOp:
                return ast.UnaryOp(unary_op_from_str[node.op](), self.visit(node.operand))
            case nodes.Assign:
                return ast.Assign([self.visit(t) for t in node.targets], self.visit(node.value))
            case nodes.AugAssign:
                return ast.AugAssign(self.visit(node.target), bin_op_from_str[node.op](), self.visit(node.value))
            case nodes.AnnAssign:
                return nodes.AnnAssign(self.visit(node.target), self.visit(node.annotation), self.visit(node.value), node.simple) 
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


 
