import ast 

from cascade.wrappers import ClassWrapper
from cascade.descriptors import ClassDescriptor, MethodDescriptor
from cascade.frontend.ast_visitors import ExtractTypeVisitor
from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph
from cascade.frontend.dataflow_analysis.cfg_builder import CFGBuilder
from cascade.frontend.dataflow_analysis.split_analyzer import SplitAnalyzer
from cascade.frontend.dataflow_analysis.split_stratagy import LinearSplitStratagy
from cascade.frontend.dataflow_analysis.split_function_builder import SplitFunctionBuilder
from cascade.frontend.dataflow_analysis.ssa_converter import SSAConverter
from cascade.frontend.ast_ import unparse
from cascade.frontend.transformers import SelfTranformer

class Compiler:

    def __init__(self, registered_classes: list[ClassWrapper]): 
        self.registered_classes: list[ClassWrapper] = registered_classes
        self.entities: list[str] = self.get_entity_names()
            
    def compile_registered_classes(self) -> str:
        for cls in self.registered_classes:
            self.compile_class(cls)

    def compile_class(self, cls: ClassWrapper):
        cls_desc: ClassDescriptor = cls.class_desc
        print()
        for method_desc in cls_desc.methods_dec:
            if method_desc.method_name == '__init__':
                continue
            print('-'*100)
            print(f'COMPILING {method_desc.method_name}')
            print('-'*100)
            self.compile_method(method_desc)
            print('\n\n')

    def compile_method(self, method_desc: MethodDescriptor):
        """ Compiles class method, executes multiple passes.
            [1] Extract instance type map.
            [2] Create control flow graph.
            [3] Split blocks with given split stratagy.
        """
        # Convert to SSA
        converter = SSAConverter(method_desc.method_node)
        ssa_ast = converter.convert()
        method_desc.set_method_node(ssa_ast)
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
        split_builder: SplitFunctionBuilder = SplitFunctionBuilder(cfg, method_desc.method_name)
        split_builder.build_split_functions()

        # Change self to state in split functions.
        for f in split_builder.functions:
            SelfTranformer().visit(f)
            # set lineno and col_offset for generated nodes.
            # ast.fix_missing_locations(f)
        #TODO: change self into state in split functions.
        # pass 5: Create dataflow graph.
        self.print_split_functions(split_builder.functions)


    def print_split_functions(self, functions):
        res = ''
        for f in functions:
            s = unparse(f)
            res += s + '\n\n'
        print(res)

    def get_entity_names(self) -> str:
        """Returns a list with the names of all registered entities"""
        return [cls.class_desc.class_name for cls in self.registered_classes]

    @classmethod
    def compile(cls, registered_classes: list[ClassWrapper]):
        compiler = cls(registered_classes)
        compiler.compile_registered_classes()


