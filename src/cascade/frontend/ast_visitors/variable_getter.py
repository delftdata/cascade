import ast

from cascade.frontend.ast_ import SSAName


class VariableGetter(ast.NodeVisitor):
    """get all variables (ast.name) from given node, separate by targets and values
     
    """

    def __init__(self):
        self.targets = []
        self.values = []
        self.current_loc = self.values  # default location to store the found variable

    @classmethod
    def get_variable(cls, node: ast.AST):
        c = cls()
        c.visit(node)
        return c

    def visit_Name(self, node: ast.Name):
        self.values.append(node)
    
    def visit_SSAName(self, node: SSAName):
        self.values.append(node)

    def visit_Assign(self, node: ast.Assign):
        self.targets.extend(node.targets)
        self.visit(node.value)
