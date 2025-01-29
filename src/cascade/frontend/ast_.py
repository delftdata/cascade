import ast

ast.unparse

ast.Assign


class SSAName(ast.Name):
    def __init__(self, id, version=-1, ctx = ..., **kwargs):
        super().__init__(id, ctx, **kwargs)
        self.version: int = version

class SSAUnparser(ast._Unparser):

    def visit_SSAName(self, node):
        if node.version == -1:
            self.write(node.id)
        self.write(f'{node.id}_{node.version}')
    
    def get_type_comment(self, node):
        return None

def unparse(ast_obj):
    unparser = SSAUnparser()
    return unparser.visit(ast_obj)
