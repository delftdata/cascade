from textwrap import indent
from typing import Any, Callable, Union, TYPE_CHECKING


from cascade.frontend.cfg import Statement
from cascade.frontend.ast_visitors.replace_name import ReplaceSelfWithState
from cascade.frontend.generator.unparser import unparse
from cascade.dataflow.dataflow import CallEntity, CallLocal, DataFlow, DataflowRef, InvokeMethod

from klara.core.cfg import RawBasicBlock
from klara.core import nodes

if TYPE_CHECKING:
    from cascade.dataflow.operator import MethodCall, StatelessMethodCall


def to_entity_call(statement: Statement, type_map: dict[str, str], dataflows: dict[DataflowRef, DataFlow]) -> CallEntity:
    """Transform a remote statement to an entity call."""
    writes = statement.targets
    assert statement.is_remote()
    assert len(writes) <= 1
    if len(writes) == 0:
        assign = None
    else:
        assign = list(writes)[0]

    # repr includes version 
    operator_var, dataflow_name = repr(statement.attribute.value), statement.attribute.attr

    if operator_var in type_map:
        operator_name = type_map[operator_var]
        key = repr(statement.attribute.value)
    else:
        # assume stateless operator
        operator_name = operator_var
        key = None

    dataflow = DataflowRef(operator_name, dataflow_name)
    
    args = statement.values.copy()
    args.remove(operator_var)
    df_args = dataflows[dataflow].args

    return CallEntity(dataflow, {a: b for a, b in zip(df_args, args, strict=True)}, assign_result_to=assign,keyby=key)


class LocalBlock:
    def __init__(self, statements: list[Statement], method_base_name: str, block_num: int, class_name: str):
        assert len(statements) > 0
        # A block of statements should have no remote calls 
        assert all([not s.is_remote() for s in statements])

        self.statements: list[Statement] = statements
        self.method_base_name: str = method_base_name
        self.block_num: int = block_num
        self.class_name: str = class_name

        writes, reads = set(), set()
        for s in statements:
            if type(s.block) != nodes.FunctionDef:
                writes.update(t for t in s.targets)
                reads.update(v for v in s.values)

        # If we assign a variable inside a function
        # that means this variable can only have been assigned in this function,
        # thanks to SSA. Thus we can remove it from reads, as it is local.
        reads.difference_update(writes)

        # Additionally, writes with higher versions will override writes
        # with lower versions.
        # e.g. a_0 = 2
        # a_1 = 4
        # we want to remove a_0 from writes, as it will never be read by future
        # blocks

        # writes.update

        self.reads: set[str] = reads
        self.writes: set[str] = writes

    def compile(self) -> 'CompiledLocalBlock':
        return CompiledLocalBlock(self)
    
    def compile_function(self) -> Callable:
        local_scope = {}
        exec(self.to_string(), {}, local_scope)
        method_name = self.get_method_name()
        return local_scope[method_name]

    def to_node(self) -> CallLocal:
        return CallLocal(InvokeMethod(self.get_method_name()))

    def get_method_name(self):
        return f"{self.method_base_name}_{self.block_num}"

    def to_string(self) -> str:
        indent_prefix: str = ' ' * 4 # indent using 4 spaces.
        body: str = indent(self.body_to_string(), indent_prefix)
        method_signature: str = self.get_method_signature()
        compiled_method_as_string: str = f'def {self.get_method_name()}({method_signature}):\n{body}'
        return compiled_method_as_string

    def get_method_signature(self) -> str:
        return f'variable_map, state'  

    def body_to_string(self) -> str:
        body = []

        # Read from the variable map
        for v in sorted(self.reads - self.writes):
            if not (v in [ 'self_0','self']):
                body.append(f'{v} = variable_map[\'{v}\']')

        # Write statements
        for statement in self.statements:
            block: RawBasicBlock = statement.block
            if type(block) == nodes.FunctionDef:
                continue

            # TODO: do this in preprocessing
            ReplaceSelfWithState.replace(block)
            
            body.append(unparse(block))

        if 'return' not in body[-1]:
            # Write to the variable map
            for v in sorted(self.writes - self.reads):
                if not (v in [ 'self_0','self']):
                    body.append(f'variable_map[\'{v}\'] = {v}')
            body.append('return None')
        return "\n".join(body)
    

class CompiledLocalBlock:
    def __init__(self, block: LocalBlock):
        self.method_base_name: str = block.method_base_name
        self.block_num: int = block.block_num
        self.class_name: str = block.class_name

        self.reads = block.reads
        self.writes = block.writes
        self.function_string = block.to_string()
        self.function: Union['MethodCall', 'StatelessMethodCall'] = block.compile_function()

    def call_block(self, *args, **kwargs) -> Any:
        return self.function(*args, **kwargs)
    

    # def to_node(self) -> CallLocal:
    #     return CallLocal(InvokeMethod(self.get_method_name()))

    def get_method_name(self):
        return f"{self.method_base_name}_{self.block_num}"

    # def get_method_signature(self) -> str:
    #     return f'variable_map, state'  
