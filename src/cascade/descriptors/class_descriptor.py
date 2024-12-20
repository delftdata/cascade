
from klara.core import nodes

from cascade.frontend.ast_visitors import ExtractClassDefNode

class ClassDescriptor:
    """A description of a class method."""

    def __init__(
        self,
        class_name: str,
        module_node: nodes.Module,
        class_node: nodes.ClassDef,
        # methods_dec: List[MethodDescriptor],
        # expression_provider,
    ):
        self.class_name: str = class_name
        self.module_node: nodes.Module = module_node
        self.class_node: nodes.ClassDef = class_node
        # self.methods_dec: List[MethodDescriptor] = methods_dec
        # self.expression_provider = expression_provider


    @classmethod
    def from_module(cls, class_name: str, module_node: nodes.Module):
        class_node = nodes.ClassDef = ExtractClassDefNode.extract(module_node, class_name)
        c = cls(class_name, module_node, class_node)
        return c