from klara.core.ssa_visitors import AstVisitor
from klara.core import nodes

class VariableGetter(AstVisitor):
    """get all variables (ast.name) from given node, separate by targets and values
     
    """

    def __init__(self):
        self.targets = []
        self.values = []
        self.current_loc = self.values  # default location to store the found variable

    @classmethod
    def get_variable(cls, node):
        c = cls()
        c.visit(node)
        return c

    def visit_name(self, node):
        self.values.append(node)

    def visit_assignname(self, node):
        self.targets.append(node)

    def visit_assignattribute(self, node: nodes.AssignAttribute):
        self.targets.append(node.value)
