from typing import Union
from klara.core.ssa_visitors import AstVisitor
from klara.core import nodes

class ReplaceSelfWithState(AstVisitor):
    """Replace attributes with "self" into "__state", and remove SSA versioning.

    e.g.:
    self_0.balance_0 -> __state['balance']
    """

    def __init__(self):
        self.target: str = "self"
        self.new: str = "__state"

    @classmethod
    def replace(cls, node):
        c = cls()
        c.visit(node)
        return c

    def replace_name(self, node: nodes.Name):
        node.id = self.new
        node.version = -1

    def replace_node(self, parent: nodes.BaseNode, old_node: nodes.BaseNode, new_node: nodes.BaseNode):
        # get node children
        for field in parent._fields:
            attr = getattr(parent, field)
            if isinstance(attr, (tuple, list)):
                to_change = None
                for i, n in enumerate(attr):
                    if n == old_node:
                        to_change = i

                if to_change is not None:
                    if isinstance(attr[i], tuple):
                        new_attr = list(attr)
                        new_attr[i] = new_node
                        attr = tuple(new_attr)
                    else:
                        attr[i] = new_node
                    setattr(parent, field, attr)
            else:
                if attr is not None:
                    if attr == old_node:
                        setattr(parent, field, new_node)
                else:
                    continue


    def replace_attribute(self, node: Union[nodes.Attribute, nodes.AssignAttribute]):
        # change self -> state
        node.value.id = self.new
        node.value.version = -1

        # change attribute to subscript
        new_node = nodes.Subscript(node.lineno, None, node.parent, node.links, version=-1)
        slice = nodes.Index(new_node.lineno, None, new_node)
        slice.postinit(nodes.Const(node.attr, slice.lineno, slice.col_offset, slice))
        new_node.postinit(node.value, slice, node.ctx)
        assert isinstance(node.parent, nodes.BaseNode)
        self.replace_node(node.parent, node, new_node)
 

    def visit_subscript(self, node: nodes.Subscript):
        # e.g. self_0.data["something"]_0 -> state.data["something"]
        if isinstance(node.value, nodes.Attribute):
            attr = node.value
            if str(attr.value) == self.target:
                self.replace_attribute(attr)
                node.version = -1
        
    def visit_assignattribute(self, node: nodes.AssignAttribute):
        if str(node.value) == self.target:
            self.replace_attribute(node)

    
    def visit_attribute(self, node: nodes.Attribute):
        if str(node.value) == self.target:
            self.replace_attribute(node)

