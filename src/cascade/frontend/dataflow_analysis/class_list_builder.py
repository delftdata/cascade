
from klara.core.ssa_visitors import AstVisitor
from klara.core.nodes import ClassDef
from klara.core.nodes import ClassDef, Module
from klara.core.cfg import Cfg, RawBasicBlock, FunctionLabel


from cascade.frontend.dataflow_analysis.class_list import ClassList
from cascade.frontend.dataflow_analysis.class_wrapper import ClassWrapper
from cascade.frontend.dataflow_analysis.dataflow_graph_build_context import DataflowGraphBuildContext
from cascade.frontend.dataflow_analysis.extract_method_visitor import ExtractMethodVisitor
from cascade.frontend.ast_visitors import ExtractTypeVisitor, ExtractEntityVisitor


class ClassListBuilder(AstVisitor):

    def __init__(self, entity_list):
        self.class_list: ClassList = ClassList()
        self.entity_list = entity_list
    
    def visit_classdef(self, node: ClassDef):
        name: str = str(node.name)
        type_map: dict[str, str] = ExtractTypeVisitor.extract(node)
        dataflow_graph_build_context: DataflowGraphBuildContext = DataflowGraphBuildContext(name, self.entity_list, type_map)
        methods = ExtractMethodVisitor.extract(node, dataflow_graph_build_context)
        attributes = {} #TODO: Fill attributes
        class_wrapper: ClassWrapper = ClassWrapper(name, methods, attributes)
        self.class_list.append(class_wrapper)
    
    def visit_cfg(self, cfg: Cfg):
        for block in cfg.block_list:
            block: RawBasicBlock
            for node in block.ssa_code:
                if type(node) in [Module]:
                    continue
                self.visit(node)

    @classmethod
    def build(cls, cfg):
        entity_list = ExtractEntityVisitor.extract(cfg)
        class_list_builder = cls(list(entity_list.keys()))
        class_list_builder.visit_cfg(cfg)
        return class_list_builder.class_list
