from inspect import isclass, getsource, getfile
from typing import Dict

from klara.core import nodes
from klara.core.tree_rewriter import AstBuilder
from klara.core.cfg import Cfg


from cascade.wrappers import ClassWrapper
from cascade.descriptors import ClassDescriptor, MethodDescriptor
from cascade.frontend.generator.generate_split_functions import GenerateSplittFunctions
from cascade.frontend.generator.generate_dataflow import GenerateDataflow
from cascade.dataflow.dataflow import DataFlow 
from cascade.frontend.intermediate_representation import StatementDataflowGraph
from cascade.frontend.generator.build_compiled_method_string import BuildCompiledMethodsString
from cascade.frontend.ast_visitors import ExtractTypeVisitor

def setup_cfg(code: str) -> Cfg:
        as_tree = AstBuilder().string_build(code)
        cfg = Cfg(as_tree)
        cfg.convert_to_ssa()
        return cfg, as_tree


parse_cache: Dict[str, nodes.Module] = {}

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
                parsed_cls, tree = setup_cfg(to_parse_file)
                parse_cache[class_file_name] = (parsed_cls, tree)
        else:
            parsed_cls, tree = parse_cache[class_file_name]
    else:
        class_source = getsource(cls)
        parsed_cls, tree = setup_cfg(class_source)

    # Create class descripter for class
    class_desc: ClassDescriptor = ClassDescriptor.from_module(cls.__name__, tree)
    class_wrapper: ClassWrapper = ClassWrapper(cls, class_desc)
    registered_classes.append(class_wrapper)


def init():
    for cls in registered_classes:
        for method in cls.class_desc.methods_dec:
            method.build_dataflow()


def get_entity_names() -> str:
    """Returns a list with the names of all registered entities"""
    return [cls.class_desc.class_name for cls in registered_classes]


def get_compiled_methods() -> str:
    """Returns a list with the compiled methods as string"""
    compiled_methods: list[str] = []
    entities: list[str] = get_entity_names()
    for cls in registered_classes:
        cls_desc: ClassDescriptor = cls.class_desc
        for method_desc in cls_desc.methods_dec:
            if method_desc.method_name == '__init__':
                continue
            dataflow_graph: StatementDataflowGraph = method_desc.dataflow
            instance_type_map: dict[str, str] = ExtractTypeVisitor.extract(method_desc.method_node)
            split_functions = GenerateSplittFunctions.generate(dataflow_graph, cls_desc.class_name, entities, instance_type_map)
            df: DataFlow = GenerateDataflow.generate(split_functions, instance_type_map)
            class_compiled_methods: str = BuildCompiledMethodsString.build(split_functions)
            compiled_methods.append(class_compiled_methods)

    return '\n\n'.join(compiled_methods)


def clear():
    registered_classes.clear()
    parse_cache.clear()
