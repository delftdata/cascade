import ast

class ContainsAttributeVisitor(ast.NodeVisitor):

    def __init__(self):
        self.contains_attribute: bool = False
        self.attribute = None
    
    def visit_Attribute(self, node: ast.Attribute):
        assert self.contains_attribute == False, "Assuming only one attribute accesed per statement"
        self.contains_attribute = True
        self.attribute: ast.Attribute = node
    
    @classmethod
    def check(cls, node):
        c = cls()
        c.visit(node)
        id_ = None
        if c.attribute:
            id_ = c.attribute.id
        return c.contains_attribute, id_

    @classmethod
    def check_and_return_attribute(cls, node):
        c = cls()
        c.visit(node)
        return c.contains_attribute, c.attribute
