from cascade.wrappers import ClassWrapper
from cascade.descriptors import ClassDescriptor, MethodDescriptor
from cascade.frontend.ast_visitors import ExtractTypeVisitor
from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph
from cascade.frontend.dataflow_analysis.cfg_builder import CFGBuilder
from cascade.frontend.dataflow_analysis.split_analyzer import SplitAnalyzer
from cascade.frontend.dataflow_analysis.split_stratagy import LinearSplitStratagy

class Compiler:

    def __init__(self, registered_classes: list[ClassWrapper]): 
        self.registered_classes: list[ClassWrapper] = registered_classes
        self.entities: list[str] = self.get_entity_names()
            
    def compile_registered_classes(self) -> str:
        for cls in self.registered_classes:
            self.compile_class(cls)

    def compile_class(self, cls: ClassWrapper):
        cls_desc: ClassDescriptor = cls.class_desc
        for method_desc in cls_desc.methods_dec:
            if method_desc.method_name == '__init__':
                continue
            self.compile_method(method_desc)

    def compile_method(self, method_desc: MethodDescriptor):
        """ Compiles class method, executes multiple passes.
            [1] Extract instance type map.
            [2] Create control flow graph.
            [3] Split blocks with given split stratagy.
        """
        # Instance type map captures which instances are of which types entity type.
        instance_type_map: dict[str, str] = ExtractTypeVisitor.extract(method_desc.method_node)
        # pass 2: create cfg.
        cfg: ControlFlowGraph = CFGBuilder.build_cfg(method_desc.method_node.body)
        # pass 3: create split functions of cfg blocks.
        split_analyzer: SplitAnalyzer = SplitAnalyzer(cfg, LinearSplitStratagy(self.entities, instance_type_map))
        split_analyzer.split()
        # pass 4: Create split functions from control flow graph.
        # Keep the code in ast form (do not convert to string) and add the remote function 
        # invocations to the function bodies.

        # pass 5: Create dataflow graph.

    def get_entity_names(self) -> str:
        """Returns a list with the names of all registered entities"""
        return [cls.class_desc.class_name for cls in self.registered_classes]

    @classmethod
    def compile(cls, registered_classes: list[ClassWrapper]):
        compiler = cls(registered_classes)
        compiler.compile_registered_classes()


