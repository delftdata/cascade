from itertools import count
import ast

from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph
from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, Block, SplitBlock, IFBlock
from cascade.frontend.dataflow_analysis.cfg_visiter import CFGVisitor


#TODO: add arguments to split functions...


class SplitFunctionBuilder(CFGVisitor):
    """ Takes a "ControlFlowGraph" and build split functions.
    """

    def __init__(self, cfg: ControlFlowGraph, method_name: str):
        self.cfg: ControlFlowGraph = cfg
        self.method_name: str = method_name
        self.functions: list[ast.FunctionDef] = []
        self.counter = count()
        self.if_cond_counter = count()
        self.split_counter = count()
    
    def build_split_functions(self):
        self.visit_blocks(self.cfg.blocks)
    
    def visit_block(self, block: Block):
        """ Put the blocks statements into a function.
        """
        args = self.get_function_args()
        self.add_new_function(block.statements, args, f'{self.method_name}_{next(self.counter)}')
    
    def visit_ifblock(self, block: IFBlock):
        if_cond_num: int = next(self.if_cond_counter)
        #TODO: Add return stmnt around tests and init vars from variable map
        self.add_new_function([ast.Return(block.test)], [], f'{self.method_name}_if_cond_{if_cond_num}')
        self.visit_generic_block(block.body)
        self.visit_generic_block(block.or_else)
    
    def visit_splitblock(self, block: SplitBlock):
        """ Add split block to function. Add remote function calls to key stack
        """
        key_stack_call: ast.Statement = self.transform_remote_call_to_callstack(block.remote_function_calls)
        if block.statements:
            self.add_new_function(block.statements + [key_stack_call], [], f'{self.method_name}_split_{next(self.split_counter)}')
    
    def transform_remote_call_to_callstack(self, remote_calls: list[ast.stmt]):
        """ Transforms a remote entity invocation. Appends right key to callstack.
        """
        return ast.stmt()

    def add_new_function(self, statements: list[ast.stmt], args, method_name: str):
        """ Build new function and add to function list.
        """
        new_function = ast.FunctionDef(
            method_name,
            args,
            statements,
            [],
            [],
            lineno=0) # returns argumentss
        self.functions.append(new_function)
    
    @staticmethod
    def get_function_args() -> ast.arguments:
        arg_list: list[ast.Arg] = SplitFunctionBuilder.arg_list()
        return ast.arguments(posonlyargs=[], args=arg_list, kwonlyargs=[], defaults=[])
    
    @staticmethod
    def arg_list():
        arg_list: list[str] = ['variable_map', 'state', 'key_stack']
        return [ast.arg(a) for a in arg_list]
    
    @staticmethod
    def extract_variables_from_statment_list(statements: list[ast.stmt]):
        """
        """

