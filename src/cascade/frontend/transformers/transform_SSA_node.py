import ast

from cascade.frontend.ast_ import SSAName

class TransformSSANode(ast.NodeTransformer):

    def visit_SSAName(self, node: SSAName):
        return ast.Name(id=f'{node.id}_{node.version}', ctx=node.ctx)
