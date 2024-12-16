from klara.core.ssa_visitors import AstVisitor
from klara.core.nodes import Attribute

class ReplaceName(AstVisitor):
    """get all variables (ast.name) from given node, separate by targets and values
     
    """

    def __init__(self, target: str, new: str):
        self.target: str = target
        self.new: str = new

    @classmethod
    def replace(cls, node, target: str, new: str):
        c = cls(target, new)
        c.visit(node)
        return c

    def visit_attribute(self, node: Attribute):
        node.value.id = 'state'