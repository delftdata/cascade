import ast


class MethodDescriptor:
    """A descriptor of a class method"""
    
    def __init__(
            self,
            method_name: str,
            method_node: ast.FunctionDef,
    ):
        self.method_name: str = method_name
        self.method_node: ast.FunctionDef = method_node
