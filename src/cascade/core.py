from inspect import isclass, getsource, getfile
from typing import Dict

from klara.core import nodes

from cascade.dataflow.operator import StatefulOperator, StatelessOperator, Operator
from cascade.preprocessing import setup_cfg
from cascade.wrappers import ClassWrapper
from cascade.descriptors import ClassDescriptor
from cascade.frontend.generator.dataflow_builder import  DataflowBuilder
from cascade.dataflow.dataflow import CallLocal, DataFlow, DataflowRef, InitClass 



parse_cache: Dict[str, nodes.Module] = {}

registered_classes: list[ClassWrapper] = []

operators: dict[str, Operator] = {}
dataflows: dict[DataflowRef, DataFlow] = {}

def cascade(cls=None, *, parse_file=True, globals=None):

    def decorator(cls):
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
        class_desc: ClassDescriptor = ClassDescriptor.from_module(cls.__name__, tree, globals)
        class_wrapper: ClassWrapper = ClassWrapper(cls, class_desc)
        registered_classes.append(class_wrapper)

    # Support both @cascade and @cascade(globals={...})
    if cls is None:
        return decorator
    return decorator(cls)


def init():
    # First pass: register operators/classes
    for cls in registered_classes:
        op_name = cls.class_desc.class_name

        if cls.class_desc.is_stateless:
            op = StatelessOperator(cls.cls, {}, {})
        else:
            op = StatefulOperator(cls.cls, {}, {})

        op: Operator = op

        # generate split functions
        for method in cls.class_desc.methods_dec:
            df_ref = DataflowRef(op_name, method.method_name)
            # Add version number manually
            args = [f"{str(arg)}_0" for arg in method.method_node.args.args]
            # TODO: cleaner solution that checks if the function is stateful or not
            if len(args) > 0 and args[0] == "self_0":
                args = args[1:]
            dataflows[df_ref] = DataFlow(method.method_name, op_name, args)

        operators[op_name] = op
    
    # Second pass: build dataflows
    for cls in registered_classes:
        op_name = cls.class_desc.class_name
        op = operators[op_name]

        # generate split functions
        for method in cls.class_desc.methods_dec:
            if method.method_name == "__init__":
                df = DataFlow("__init__", op_name)
                n0 = CallLocal(InitClass())
                df.entry = [n0]
                blocks = []
            else:
                df = DataflowBuilder(method.method_node, dataflows, cls.class_desc.globals).build(op_name)
            
            dataflows[df.ref()] = df
            op.dataflows[df.ref()] = df
            for name, b in df.blocks.items():
                op.methods[name] = b


def get_operator(op_name: str):
    return operators[op_name]

def get_dataflow(ref: DataflowRef):
    return dataflows[ref]


def clear():
    registered_classes.clear()
    parse_cache.clear()
