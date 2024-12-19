from textwrap import indent
from dataclasses import dataclass, field


from cascade.frontend.util import to_camel_case
from cascade.frontend.intermediate_representation import Statement
from cascade.frontend.ast_visitors.replace_name import ReplaceName
from cascade.frontend.generator.unparser import unparse
from cascade.frontend.generator.remote_call import RemoteCall

from klara.core.cfg import RawBasicBlock
from klara.core import nodes

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
        body: str = indent(self.body_to_string(), '\t')
        method_signature: str = self.get_method_signature()
        compiled_method_as_string: str = f'def {self.method_name}_compiled({method_signature}) -> Any: \n {body}'
        return compiled_method_as_string

    def get_method_signature(self) -> str:
        return f'variable_map: dict[str, Any], state: {self.class_name}, key_stack: list[str]'  

    def body_to_string(self) -> str:
        body = []
        for v in self.values:
            if not (v in [ 'self_0','self']):
                body.append(f'{v} = variable_map[\'{v}\']')

        for statement in self.method_body:
            if statement.remote_call:
                assert statement.attribute_name
                instance_name = statement.attribute_name
                res = f'key_stack.append(variable_map[\'{instance_name}_key\'])'
                body.append(res)
            else:
                block: RawBasicBlock = statement.block
                if type(block) == nodes.FunctionDef:
                    continue
                ReplaceName.replace(block, 'self', 'state')
                
                if type(block) == nodes.Return:
                    body.append('key_stack.pop()') 
                body.append(unparse(block))
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


