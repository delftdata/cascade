from klara.core.ssa_visitors import AstVisitor
from klara.core.nodes import AnnAssign, Arg


class ExtractTypeVisitor(AstVisitor):

    def __init__(self):
        """ The type map keeps track of variable types for methods.
            e.g. : item: Item. {"item": "Item"}
        """
        self.type_map: dict[str, str] = {}

    def visit_annassign(self, node: AnnAssign):
        id: str = str(node.target.id)
        type_: str = str(node.annotation.id)
        self.type_map[id] = type_
    
    def visit_arg(self, arg: Arg):
        annotation = arg.annotation
        if annotation != None:
            id: str = arg.arg
            self.type_map[id] = str(annotation.id)
    
    def get_type_map(self) -> dict[str, str]:
        return self.type_map
    
    @classmethod
    def extract(cls, node):
        extract_type_visitor: ExtractTypeVisitor = cls()
        extract_type_visitor.visit(node)
        return extract_type_visitor.get_type_map()
