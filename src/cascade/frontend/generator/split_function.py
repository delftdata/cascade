from textwrap import indent
from dataclasses import dataclass, field
from typing import Union


from cascade.dataflow.dataflow import CallEntity, CallLocal, DataFlow, DataflowRef, InvokeMethod
from cascade.dataflow.operator import Block
from cascade.frontend.util import to_camel_case
from cascade.frontend.intermediate_representation import Statement
from cascade.frontend.ast_visitors.replace_name import ReplaceSelfWithState
from cascade.frontend.generator.unparser import unparse
from cascade.frontend.generator.remote_call import RemoteCall

from klara.core.cfg import RawBasicBlock
from klara.core import nodes
from klara.core.node_classes import Name

@dataclass
class SplitFunction:
    method_number: int
    method_name: str
    method_body: list[Statement]
    targets: set[str] = None
    values: set[str] = None
    class_name: str = None
    remote_calls: list[RemoteCall] = field(default_factory=list) # {'assign_result_to_var': 'method_to_call'}

    def set_class_name(self, name: str):
        self.class_name = name
    
    def to_string(self) -> str:
        indent_prefix: str = ' ' * 4 # indent usting 4 spaces.
        body: str = indent(self.body_to_string(), indent_prefix)
        method_signature: str = self.get_method_signature()
        compiled_method_as_string: str = f'def {self.method_name}_compiled({method_signature}) -> Any:\n{body}'
        return compiled_method_as_string

    def get_method_signature(self) -> str:
        return f'variable_map: dict[str, Any], state: {self.class_name}, key_stack: list[str]'  

    def body_to_string(self) -> str:
        body = []
        for v in sorted(self.values - self.targets):
            if not (v in [ 'self_0','self']):
                body.append(f'{v} = variable_map[\'{v}\']')

        for statement in self.method_body:
            if statement.remote_call:
                assert statement.attribute
                attribute: nodes.Attribute = statement.attribute
                value: nodes.Name = attribute.value
                instance_name: str = value.id
                res = f'key_stack.append(variable_map[\'{instance_name}_key\'])'
                body.append(res)
            else:
                block: RawBasicBlock = statement.block
                if type(block) == nodes.FunctionDef:
                    continue
                ReplaceSelfWithState.replace(block)
                
                if type(block) == nodes.Return:
                    body.insert(0,'key_stack.pop()') 
                body.append(unparse(block))

        if 'return' not in body[-1]:
            body.append('return None')
        return "\n".join(body)

    def extract_remote_method_calls(self):
        for statement in self.method_body:
            if statement.remote_call:
                self.add_statement_to_remote_call_set(statement)

    def add_statement_to_remote_call_set(self, statement: Statement):
        assert statement.attribute, "A remote call should have an attribute name to call"
        attribute = statement.attribute
        if len(statement.targets) > 1:
            assert False, "A remote method invocation that returns multiple items is not supported yet..."
        target, = statement.targets
        remote_call: RemoteCall = RemoteCall(attribute.value.id, attribute.attr, target)
        self.remote_calls.append(remote_call)


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


class SplitFunction2:
    def __init__(self, statements: list[Statement], method_base_name: str, block_num: int, class_name: str):
        assert len(statements) > 0
        # A block of statements should have no remote calls 
        assert all([not s.is_remote() for s in statements])

        self.statements = statements
        self.method_base_name = method_base_name
        self.class_name = class_name
        self.block_num = block_num

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

        self.reads = reads
        self.writes = writes


    def to_block(self) -> tuple[CallLocal, Block]:
        local_scope = {}
        raw_str = self.to_string()
        exec(self.to_string(), {}, local_scope)
        method_name = self.get_method_name()
        fn = local_scope[method_name]
        return CallLocal(InvokeMethod(method_name)), Block(list(self.writes), list(self.reads), method_name, fn, raw_str)

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
            ReplaceSelfWithState.replace(block)
            
            body.append(unparse(block))

        if 'return' not in body[-1]:
            # Write to the variable map
            for v in sorted(self.writes - self.reads):
                if not (v in [ 'self_0','self']):
                    body.append(f'variable_map[\'{v}\'] = {v}')
            body.append('return None')
        return "\n".join(body)