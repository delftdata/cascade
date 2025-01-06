import itertools
import ast


def compare_ast(node1, node2):
    """
        Compares twoo ast's on equality.
    
        sourced from:  https://stackoverflow.com/questions/3312989/elegant-way-to-test-python-asts-for-equality-not-reference-or-object-identity
    """
    if type(node1) is not type(node2):
        return False
    if isinstance(node1, ast.AST):
        for k, v in vars(node1).items():
            if k in ('lineno', 'col_offset', 'ctx', 'end_lineno'):
                continue
            if not compare_ast(v, getattr(node2, k)):
                return False
        return True
    elif isinstance(node1, list):
        return all(itertools.starmap(compare_ast, zip(node1, node2)))
    else:
        return node1 == node2


class MethodExtracter(ast.NodeVisitor):
    
    def __init__(self):
        self.methods: dict[str, ast.AST] = {}

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self.methods[node.name] = node
    
    @classmethod
    def extract(cls, node: ast.AST):
        c = cls()
        c.visit(node)
        return c.methods
