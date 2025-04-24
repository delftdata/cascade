from typing import Any
from klara.core.ssa_visitors import AstVisitor
from klara.core import nodes

def simplify_returns(node):
    sr = SimplifyReturns.replace(node)

    # Add the new assign nodes to the parent after the ast visit is complete
    for parent, n, target in sr.new_nodes:
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
    """Put return statments in ANF form.

    Examples:

    `return x+3`            ->  `__ret_0 = x + 3; return __ret_0`
    `return self.balance`   ->  `__ret_1 = self.balance; return __ret_1`
    `return cat`            ->  `return cat`
    """
       
    def __init__(self):
        self.temps = 0
        # (return_node parent block, modified return_node, new assign_node)
        self.new_nodes: list[tuple[Any, nodes.Return, nodes.Assign]] = []

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
        self.new_nodes.append((node.parent, node, new_assign))

    def visit_return(self, node: nodes.Return):

        if not isinstance(node.value, nodes.Name):
            self.replace_name(node)
   
