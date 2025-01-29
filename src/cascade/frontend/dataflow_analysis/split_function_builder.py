from itertools import count
import ast

from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph
from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, Block, SplitBlock, IFBlock
from cascade.frontend.dataflow_analysis.cfg_visiter import CFGVisitor
from cascade.frontend.ast_visitors import ContainsAttributeVisitor
from cascade.frontend.ast_visitors import VariableGetter


KEY_STACK: str = 'key_stack'
VARIABLE_MAP: str = 'variable_map'
STATE: str = 'state'
ARG_LIST: list[str] = [VARIABLE_MAP, STATE, KEY_STACK]

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
        self.breadth_first_walk(self.cfg)
    
    def visit_block(self, block: Block):
        """ Put the blocks statements into a function.
        """
        self.add_new_function(block.statements, f'{self.method_name}_{next(self.counter)}')
    
    def visit_ifblock(self, block: IFBlock):
        if_cond_num: int = next(self.if_cond_counter)
        self.add_new_function([ast.Return(block.test)], f'{self.method_name}_if_cond_{if_cond_num}')
    
    def visit_splitblock(self, block: SplitBlock):
        """ Add split block to function. Add remote function calls to key stack
        """
        key_stack_call: ast.Expr = self.transform_remote_call_to_callstack(block.remote_function_calls)
        self.add_new_function(block.statements + [key_stack_call], f'{self.method_name}_split_{next(self.split_counter)}')
    
    def transform_remote_call_to_callstack(self, remote_calls: list[ast.stmt]) -> ast.Expr:
        """ Transforms a remote entity invocation. Appends right key to callstack.
            returns ast of: 'key_stack.append(variable_map['{remote_call_key}'])'
        """
        if len(remote_calls) == 1:
            call, = remote_calls
            contains_attribute, attribute = ContainsAttributeVisitor.check_and_return_attribute(call)
            assert contains_attribute, "Remote function call should contain an attribute"   
            value = attribute.value
            remote_call_key: str = f'{value.id}_{value.version}' if hasattr(value, 'version') else value.id
            remote_call_key += '_key'
            return  ast.Expr(
                value=ast.Call(
                    func=ast.Attribute(
                        value=ast.Name(id=KEY_STACK, ctx=ast.Load()),
                        attr='append',
                        ctx=ast.Load()),
                    args=[
                        ast.Subscript(
                            value=ast.Name(id=VARIABLE_MAP, ctx=ast.Load()),
                            slice=ast.Constant(value=remote_call_key),
                            ctx=ast.Load())],
                    keywords=[]))
        else: 
            assert False, 'Invoking remote methods in parallel is not supported yet.'

    def add_new_function(self, statements: list[ast.stmt], method_name: str):
        """ Build new function and add to function list.
        """
        statements = self.extract_variables_from_statment_list(statements)
        args = self.get_function_args()
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
        """ Returns functions args.
            The arguments are the same for every split functions. 
            i.e.: (variable_map, state, key_stack)
        """
        arg_list: list[ast.Arg] = SplitFunctionBuilder.arg_list()
        return ast.arguments(posonlyargs=[], args=arg_list, kwonlyargs=[], defaults=[])
    
    @staticmethod
    def arg_list():
        return [ast.arg(a) for a in ARG_LIST]
    
    @staticmethod
    def extract_variables_from_statment_list(statements: list[ast.stmt]):
        """
        """
        values = []
        for v in statements:
            var_getter: VariableGetter = VariableGetter.get_variable(v)
            values.extend(var_getter.values)

        # String in arg list are reserved for arg list.
        ignore_values = ARG_LIST + ['self']
        values = [v for v in values if v.id not in ignore_values]
        return SplitFunctionBuilder.build_anssign_var_from_values(values) + statements

    @staticmethod
    def build_anssign_var_from_values(values: list[ast.Name]):
        return [SplitFunctionBuilder.value_to_assign(v) for v in values]

    @staticmethod
    def value_to_assign(value: ast.Name):
        id_ = f'{value.id}_{value.version}' if hasattr(value, 'version') else value.id
        return ast.Assign(
            targets=[
                ast.Name(id=id_, ctx=ast.Store())],
            value=ast.Subscript(
                value=ast.Name(id=VARIABLE_MAP, ctx=ast.Load()),
                slice=ast.Constant(value=id_),
                ctx=ast.Load()))
        

