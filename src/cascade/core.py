from inspect import isclass, getsource, getfile
from typing import List, Dict
import textwrap

from klara.core import nodes
from klara.core.tree_rewriter import AstBuilder
from klara.core.cfg import Cfg


from cascade.wrappers import ClassWrapper
from cascade.descriptors import ClassDescriptor


def setup_cfg(code: str) -> Cfg:
        as_tree = AstBuilder().string_build(code)
        cfg = Cfg(as_tree)
        cfg.convert_to_ssa()
        return cfg, as_tree


parse_cache: Dict[str, nodes.Module] = {}

registered_classes: List[ClassWrapper] = []


def cascade(cls, parse_file=True):
    if not isclass(cls):
        raise AttributeError(f"Expected a class but got an {cls}.")

    # Parse source.
    if parse_file:
        class_file_name = getfile(cls)
        if class_file_name not in parse_cache:
            with open(getfile(cls), "r") as file:
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