from inspect import isclass, getsource, getfile
from typing import Dict

from klara.core import nodes
from klara.core.tree_rewriter import AstBuilder
from klara.core.cfg import Cfg


from cascade.wrappers import ClassWrapper
from cascade.descriptors import ClassDescriptor, MethodDescriptor
from cascade.frontend.generator.generate_split_functions import GenerateSplitFunctions
from cascade.frontend.generator.generate_dataflow import GenerateDataflow
from cascade.dataflow.dataflow import DataFlow, OpNode, InvokeMethod
from cascade.frontend.intermediate_representation import StatementDataflowGraph
from cascade.frontend.generator.build_compiled_method_string import BuildCompiledMethodsString
from cascade.frontend.ast_visitors import ExtractTypeVisitor
from cascade.frontend.dataflow_analysis.split_control_flow import SplitControlFlow
from cascade.frontend.generator.dataflow_linker import DataflowLinker
from cascade.frontend.dataflow_analysis.cfg_builder import CFGBuiler

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


def get_entity_names() -> str:
    """Returns a list with the names of all registered entities"""
    return [cls.class_desc.class_name for cls in registered_classes]


def get_compiled_methods() -> str:
    entities: list[str] = get_entity_names()
    for cls in registered_classes:
        compile_class(cls, entities)

def compile_class(cls: ClassWrapper, entities: list[str]):
    cls_desc: ClassDescriptor = cls.class_desc
    for method_desc in cls_desc.methods_dec:
        if method_desc.method_name == '__init__':
            continue
        compile_method(method_desc)

def compile_method(method_desc: MethodDescriptor):
    # second pass extract cfg.
    cfg = CFGBuiler.build(method_desc.method_node.body)
    # third pass create split functions of cfg blocks.
    print(cfg)


def get_compiled_methods_old() -> str:
    """Returns a list with the compiled methods as string"""
    compiled_methods: list[str] = []
    entities: list[str] = get_entity_names()
    for cls in registered_classes:
        cls_desc: ClassDescriptor = cls.class_desc
        for method_desc in cls_desc.methods_dec:
            if method_desc.method_name == '__init__':
                continue
            instance_type_map: dict[str, str] = ExtractTypeVisitor.extract(method_desc.method_node)
            control_flow_splits = SplitControlFlow.split(method_desc.method_node, method_desc.method_name)
            split_functions = []
            control_flow_node_map: dict[str, list[list[OpNode]]]= {}
            for split in control_flow_splits.nodes():
                if split.is_if_condition:
                    # if node only exists of condition invocation.
                    if_cond_node = OpNode(cls_desc.class_name, InvokeMethod(split.method_name))
                    control_flow_node_map[split.method_name] = [[if_cond_node]]
                    continue
                
                split.build_dataflow()
                control_flow_split_split_functions = GenerateSplitFunctions.generate(split.dataflow, cls_desc.class_name, entities, instance_type_map) 
                split_functions.extend(control_flow_split_split_functions)
                node_list: list[list[OpNode]] = GenerateDataflow.generate(control_flow_split_split_functions, instance_type_map)
                control_flow_node_map[split.method_name] = node_list

            df = DataflowLinker.link(cls_desc.class_name, control_flow_node_map, control_flow_splits)
            
            # link after
                
                # maybe pass some num here
            class_compiled_methods: str = BuildCompiledMethodsString.build(split_functions)
            compiled_methods.append(class_compiled_methods)

    return '\n\n'.join(compiled_methods)


def clear():
    registered_classes.clear()
    parse_cache.clear()
