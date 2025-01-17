import ast
import contextlib
from itertools import count

from klara.core import inference, manager, nodes
from klara.core.inference import InferenceResult, inference_transform_wrapper
from klara.core.tree_rewriter import TreeRewriter

MANAGER = manager.AstManager()


@contextlib.contextmanager
def add_transform(manager, node, transform, predicate=None):
    manager.register_transform(node, transform, predicate)
    yield
    manager.unregister_transform(node, transform, predicate)


class Transformer:

        def __init__(self):
            self.nodes_to_add_to_body = []
            self.counter = count()
        
        def get_next_label(self):
            return f'new_{next(self.counter)}'

        def transform_assignname(self, node):
            node.id = "y"
            return node
            
        def insert_statement_above(self, binop: nodes.BinOp, right=True):
            scope: nodes.FunctionDef = binop.scope()
            parent: nodes.Assign = binop.get_stmt_target()[0].parent

            new_name = nodes.Name(lineno=parent.lineno-1, col_offset=parent.col_offset)

            name: str = self.get_next_label()
            new_name.postinit(name)
            new_node = nodes.Assign(parent=scope)
            new_node.refer_to_block = parent.refer_to_block
            binop.right.parent = new_node
            if right:
                new_node.postinit(targets=[new_name], value=binop.right)
            else:
                new_node.postinit(targets=[new_name], value=binop.left)

            body = scope.body
            index = body.index(parent)
            self.nodes_to_add_to_body.append((body, index, new_node))
            return new_name

        def add_nodes_to_body(self):
            offset = 0 
            while self.nodes_to_add_to_body:
                body, index, new_node = self.nodes_to_add_to_body.pop(0)
                body.insert(index + offset, new_node)
                offset += 1

        def transform_binop(self, node: nodes.BinOp):
            new_name: nodes.Name = self.insert_statement_above(node)
            node.right = new_name
            return node
        
        def flatten_binop_left(self, node: nodes.BinOp):
            new_name: nodes.Name = self.insert_statement_above(node, right=False)
            node.left = new_name
            return node
        
        def flatten_binop_right(self, node: nodes.BinOp):
            new_name: nodes.Name = self.insert_statement_above(node)
            node.right = new_name
            return node
        
        @contextlib.contextmanager
        def add_transformers(self, manager):
            node = nodes.BinOp
            transformers = [(self.transform_binop, self.check_bin_op),\
                             (self.flatten_binop_left, self.check_if_left_needs_flatten),\
                             (self.flatten_binop_right, self.check_if_right_needs_flatten)]
            for t, predicate in transformers:
                manager.register_transform(nodes.BinOp, t, predicate)
            yield
            for t, predicate in transformers:
                manager.unregister_transform(node, t, predicate)

        
        @classmethod
        def transform(cls, as_tree: ast.Module):
            transformer = cls()
            with transformer.add_transformers(MANAGER):
                new_tree = TreeRewriter().visit_module(as_tree)
                MANAGER.apply_transform(new_tree)
            transformer.add_nodes_to_body()
            return new_tree

        @staticmethod
        def check_bin_op(node: nodes.BinOp):
            return isinstance(node.right, nodes.Call) and isinstance(node.left, nodes.Call)
        
        @staticmethod
        def check_if_left_needs_flatten(node: nodes.BinOp):
            return isinstance(node.left, nodes.BinOp)
        
        @staticmethod
        def check_if_right_needs_flatten(node: nodes.BinOp):
            return isinstance(node.right, nodes.BinOp)
