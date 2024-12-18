from textwrap import indent
from dataclasses import dataclass


from cascade.frontend.util import to_camel_case
from cascade.frontend.intermediate_representation import Statement
from cascade.frontend.ast_visitors.replace_name import ReplaceName
from cascade.frontend.generator.unparser import Unparser

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

    def set_class_name(self, name: str):
        self.class_name = name

    def to_string(self) -> str:
        body = []
        for v in self.values:
            if not (v in [ 'self_0','self']):
                body.append(f'{v} = variable_map[\'{v}\']')

        for s in self.method_body:
            if s.remote_call:
                assert s.attribute_name
                instance_name = s.attribute_name
                res = f'key_stack.append(variable_map[\'{instance_name}_key\'])'
                body.append(res)
            else:
                block: RawBasicBlock = s.block
                if type(block) == nodes.FunctionDef:
                    continue
                ReplaceName.replace(block, 'self', 'state')
                
                if type(block) == nodes.Return:
                    body.append('key_stack.pop()') 
                body.append(Unparser.unparse_block(block))
        return "\n".join(body)


