import ast


class SSAName(ast.Name):
    def __init__(self, id, version=-1, ctx = ..., **kwargs):
        super().__init__(id, ctx, **kwargs)
        self.version: int = version
