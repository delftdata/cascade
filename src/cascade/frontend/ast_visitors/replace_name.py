from klara.core.ssa_visitors import AstVisitor
from klara.core import nodes

class ReplaceSelfWithState(AstVisitor):
    """Replace attributes with "self" into "state", and remove SSA versioning.

    e.g.:
    self_0.balance_0 -> state.balance
    """

    def __init__(self):
        self.target: str = "self"
        self.new: str = "state"

    @classmethod
    def replace(cls, node):
        c = cls()
        c.visit(node)
        return c

    def replace_name(self, node: nodes.Name):
        node.id = self.new
        node.version = -1

    def visit_subscript(self, node: nodes.Subscript):
        # e.g. self_0.data["something"]_0 -> state.data["something"]
        if isinstance(node.value, nodes.Attribute):
            name = node.value.value
            if str(name) == self.target:
                self.replace_name(name)
                node.version = -1
        
    def visit_assignattribute(self, node: nodes.AssignAttribute):
        if str(node.value) == self.target :
            self.replace_name(node.value)
            node.version = -1

    
    def visit_attribute(self, node: nodes.Attribute):
        if str(node.value) == self.target:
            self.replace_name(node.value)
            node.version = -1
