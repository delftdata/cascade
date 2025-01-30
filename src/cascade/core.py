from inspect import isclass, getsource, getfile
from typing import Dict
import ast

from klara.core import nodes
from klara.core.tree_rewriter import AstBuilder
from klara.core.cfg import Cfg


from cascade.wrappers import ClassWrapper
from cascade.descriptors import ClassDescriptor
from cascade.dataflow.dataflow import OpNode, InvokeMethod
from cascade.frontend.ast_visitors import ExtractTypeVisitor
from cascade.frontend.compiler import Compiler
from cascade.frontend.dataflow_analysis.ssa_converter import SSAConverter

def setup_cfg(code: str) -> Cfg:
    ast_module = ast.parse(code)
    return ast_module


parse_cache: Dict[str, ast.Module] = {}

registered_classes: list[ClassWrapper] = []


def cascade(cls, parse_file=True):
    if not isclass(cls):
        raise AttributeError(f"Expected a class but got an {cls}.")

    # Parse source.
    if parse_file:
        class_file_name = getfile(cls)
        if class_file_name not in parse_cache:
            with open(class_file_name, "r") as file:
                to_parse_file = file.read()
                # parsed_cls = AstBuilder().string_build(to_parse_file)
                parsed_cls = setup_cfg(to_parse_file)
                parse_cache[class_file_name] = parsed_cls
        else:
            parsed_cls = parse_cache[class_file_name]
    else:
        class_source = getsource(cls)
        parsed_cls = setup_cfg(class_source)

    # Create class descripter for class
    class_desc: ClassDescriptor = ClassDescriptor.from_module(cls.__name__, parsed_cls)
    class_wrapper: ClassWrapper = ClassWrapper(cls, class_desc)
    registered_classes.append(class_wrapper)


def get_entity_names() -> str:
    """Returns a list with the names of all registered entities"""
    return [cls.class_desc.class_name for cls in registered_classes]


def get_compiled_methods() -> str:
    Compiler.compile(registered_classes)


def clear():
    registered_classes.clear()
    parse_cache.clear()
