import ast

from cascade.frontend.ast_ import SSAName

class SelfTranformer(ast.NodeTransformer):
    """ Replaces self for state in splitfunctions
    """


    def visit_Name(self, node: ast.Name):
        return node

    def visit_SSAName(self, node: SSAName):
        if node.id == 'self':
            return ast.Name(id='state', ctx=node.ctx)
        return node
