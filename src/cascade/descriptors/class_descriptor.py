
from typing import Any, Optional
from klara.core import nodes

from cascade.descriptors.method_descriptor import MethodDescriptor
from cascade.frontend.ast_visitors.extract_class_def_node import ExtractClassDefNode
from cascade.frontend.ast_visitors.extract_class_methods import ExtractMethodVisitor

class ClassDescriptor:
    """A description of a class."""

    def __init__(
        self,
        class_name: str,
        module_node: nodes.Module,
        class_node: nodes.ClassDef,
        methods_dec: list[MethodDescriptor],
        globals: Optional[dict[str, Any]]
    ):
        self.class_name: str = class_name
        self.module_node: nodes.Module = module_node
        self.class_node: nodes.ClassDef = class_node
        self.methods_dec: list[MethodDescriptor] = methods_dec
        self.globals = globals
        
        self.is_stateless = True
        for method in methods_dec:
            if method.method_name == "__init__":
                self.is_stateless = False
                break

    def get_method_by_name(self, name: str):
        return next(m for m in self.methods_dec if m.method_name == name)

    @classmethod
    def from_module(cls, class_name: str, module_node: nodes.Module, globals):
        class_node: nodes.ClassDef = ExtractClassDefNode.extract(module_node, class_name)
        method_dec: list[MethodDescriptor] = ExtractMethodVisitor.extract(class_node)
        c = cls(class_name, module_node, class_node, method_dec, globals)
        return c
