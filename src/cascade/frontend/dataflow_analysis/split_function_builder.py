from itertools import count
import ast

from klara.core import nodes
from klara.core.cfg import Cfg
from klara.core.tree_rewriter import TreeRewriter

from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph
from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, Block, SplitBlock, IFBlock
from cascade.frontend.dataflow_analysis.cfg_visiter import CFGVisitor


class SplitFunctionBuilder(CFGVisitor):
    """ Takes a "ControlFlowGraph" and build split functions.
    """

    def __init__(self, cfg: ControlFlowGraph, method_name: str):
        self.cfg: ControlFlowGraph = cfg
        self.method_name: str = method_name
        self.functions: list[nodes.FunctionDef] = []
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
        self.add_new_function(block.test, [], f'{self.method_name}_if_cond_{if_cond_num}')
        self.add_new_function(block.body, [], f'{self.method_name}_if_body_{if_cond_num}')
        self.add_new_function(block.or_else, [], f'{self.method_name}_or_else_{if_cond_num}')
    
    def visit_splitblock(self, block: SplitBlock):
        """ Add split block to function. Add remote function calls to key stack
        """
        key_stack_call: nodes.Statement = self.transform_remote_call_to_callstack(block.remote_function_calls)
        self.add_new_function(block.statements + [key_stack_call], [], f'{self.method_name}_split_{next(self.split_counter)}')
    
    def transform_remote_call_to_callstack(self, remote_calls: list[nodes.Statement]):
        """ Transforms a remote entity invocation. Appends right key to callstack.
        """
        return nodes.Statement()

    def add_new_function(self, statements: list[nodes.Statement], args, method_name: str):
        """ Build new function and add to function list.
        """
        new_function = nodes.FunctionDef()
        new_function.postinit(
            method_name,
            args,
            statements,
            [],
            []) # returns argumentss
        self.functions.append(new_function)
    
    @staticmethod
    def get_function_args():
        arg_list: list[nodes.Arg] = SplitFunctionBuilder.arg_list()
        args: nodes.Arguments = nodes.Arguments()
        args.postinit([], arg_list, [], [], [], [])
        return args
    
    @staticmethod
    def arg_list():
        arg_list: list[str] = ['variable_map', 'state', 'key_stack']
        return [SplitFunctionBuilder.to_klara_arg(a) for a in arg_list]
    
    @staticmethod
    def to_klara_arg(arg_name: str):
        arg: nodes.Arg = nodes.Arg()
        arg.postinit(
            arg_name,
            None
        )
        return  arg
    
    @staticmethod
    def extract_variables_from_statment_list(statements: list[nodes.Statement]):
        """
        """

