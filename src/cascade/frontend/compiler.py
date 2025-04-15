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
from cascade.frontend.transformers import SelfTranformer, TransformSSANode
from cascade.frontend.dataflow_analysis.name_blocks import NameBlocks
from cascade.frontend.dataflow_analysis.dataflow_builder import build_dataflow
from cascade.dataflow.operator import StatefulOperator

class Compiler:

    def __init__(self, registered_classes: list[ClassWrapper]): 
        self.registered_classes: list[ClassWrapper] = registered_classes
        self.entities: list[str] = self.get_entity_names()
            
    def compile_registered_classes(self) -> str:
        operators = {}
        operator_control_flow_graphs = {}
        # For each class compile methods and build operators
        for cls in self.registered_classes:
            class_name: str = cls.class_desc.class_name 
            operator, cfgs = self.compile_class(cls)
            operators[class_name] = operator
            operator_control_flow_graphs[class_name] = cfgs
        
        # For each class build df
        dfs = {}
        for cls in self.registered_classes:
            class_name = cls.class_desc.class_name
            for method_desc in cls.class_desc.methods_dec:
                method_name: str = method_desc.method_name
                if method_name == '__init__':
                    continue
                df_name: str = f'{class_name}_{method_name}'
                cfg = operator_control_flow_graphs[class_name][method_name]
                self_operator: StatefulOperator = operators[class_name]
                instance_type_map: dict[str, str] = ExtractTypeVisitor.extract(method_desc.method_node)
                df = build_dataflow(df_name, cfg, self_operator, operators, instance_type_map)
                dfs[df_name] = df
        print(dfs)
            
        

    def compile_class(self, cls: ClassWrapper):
        cls_desc: ClassDescriptor = cls.class_desc
        print()
        split_functions = []
        cfgs = {}
        for method_desc in cls_desc.methods_dec:
            if method_desc.method_name == '__init__':
                continue
            print('-'*100)
            print(f'COMPILING {method_desc.method_name}')
            print('-'*100)
            split, cfg = self.compile_method(method_desc)
            split_functions.extend(split)
            print('\n\n')
            cfgs[method_desc.method_name] = cfg
        operator = self.operator_from_split_functions(cls.cls, split_functions) 
        return operator, cfgs
    
    def operator_from_split_functions(self, entity, split_functions: list[ast.FunctionDef]):
        # There are two aproaches here, for code generation, we could keep everything in ast form and
        # output the generated code to a file using an unparser.
        # However for simplicity we choose to compile the input methods and continue with python code
        # creating a df.
        module = ast.Module(
            body=split_functions,
            type_ignores=[])
        TransformSSANode().visit(module)
        ast.fix_missing_locations(module)
        m = compile(module, 'placeholder', 'exec')
        exec(m)
        # some python magic using eval to create the method dict.
        methods = {}
        for f in split_functions:
            f_name = f.name
            methods[f_name] = eval(f)
        
        return StatefulOperator(entity, methods, {})


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
        # Give each block a unique name which will be used to name the split functions and create 
        # the dataflow.
        NameBlocks(cfg, method_desc.method_name).name()
        # pass 4: Create split functions from control flow graph.
        # Keep the code in ast form (do not convert to string) and add the remote function 
        # invocations to the function bodies
        split_builder: SplitFunctionBuilder = SplitFunctionBuilder(cfg, method_desc.method_name)
        split_functions: list[ast.FunctionDef] = split_builder.build_split_functions()

        # Change self to state in split functions.
        self.transform_self_to_state(split_functions)

        # pass 5: Create dataflow graph.
        self.print_split_functions(split_builder.functions)

        return split_functions, cfg


    def transform_self_to_state(self, split_functions: list[ast.FunctionDef]):
        """ Replaces self with state.
        """
        for f in split_functions:
            SelfTranformer().visit(f)
            # set lineno and col_offset for generated nodes.
            ast.fix_missing_locations(f)

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


