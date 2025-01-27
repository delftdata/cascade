import ast 


from cascade.descriptors.method_descriptor import MethodDescriptor


class ExtractMethodVisitor(ast.NodeVisitor):

    def __init__(self):
        self.methods: dict[str, MethodDescriptor] = {}
    
    def visit_FunctionDef(self, node: ast.FunctionDef):
        name: str = str(node.name)
        assert name not in self.methods, "A method should be only defined once"
        descriptor: MethodDescriptor = MethodDescriptor(name, node)
        self.methods[name] = descriptor
    
    def visit_Function(self, node):
        print('visiting funciton node')

    @classmethod
    def extract(cls, node):
        """Node should be a top level class node"""
        c = cls()
        c.visit(node)
        return list(c.methods.values())
        