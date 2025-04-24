from klara.core.ssa_visitors import AstVisitor
from klara.core import nodes

def simplify_returns(node):
    sr = SimplifyReturns.replace(node)
    for parent, n, target in sr.inserts:
        try:
            i = parent.body.index(n)
            parent.body.insert(i, target)
        except ValueError as e:
            if isinstance(parent, nodes.If):
                i = parent.orelse.index(n)
                parent.orelse.insert(i, target)
            else:
                raise e

class SimplifyReturns(AstVisitor):
    """Replace attributes with "self" into "state", and remove SSA versioning.

    e.g.:
    self_0.balance_0 -> state.balance
    """
       
    def __init__(self):
        self.temps = 0
        self.inserts = []

    @classmethod
    def replace(cls, node):
        c = cls()
        c.visit(node)
        return c

    def replace_name(self, node: nodes.Return):
        new_assign = nodes.Assign(parent=node.parent, lineno=node.lineno)
        target = nodes.AssignName(parent=new_assign)
        target.postinit(id=f"__ret_{self.temps}")
        self.temps += 1
        new_assign.postinit(targets=[target], value=node.value)
        node.value = nodes.Name()
        node.value.postinit(target.id)
        
        assert hasattr(node.parent, "body"), type(node.parent)
        print(f"replacing {node} in {node.parent} with {new_assign}")
        self.inserts.append((node.parent, node, new_assign))

    def visit_return(self, node: nodes.Return):

        if not isinstance(node.value, nodes.Name):
            self.replace_name(node)
   
