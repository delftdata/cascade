from textwrap import indent
from dataclasses import dataclass

from cascade.frontend.util import to_camel_case

from klara.core.cfg import RawBasicBlock

@dataclass
class SplitFunction:
    method_number: int
    method_name: str
    method_body: list[RawBasicBlock]
    in_vars: set[str]
    out_vars: set[str]
    class_name: str = None

    def set_class_name(self, name: str):
        self.class_name = name

    def to_string(self) -> str:
        res = ""
        in_vars: set[str] = self.in_vars
        state_type: str = self.class_name
        method_signature = f'variable_map: dict[str, Any], state: {state_type}, key_stack: list[str]'
        vars_from_var_map = self.extract_in_vars(in_vars)
        body: str = self.method_body
        class_name_camel_case: str = to_camel_case(self.class_name)
        if in_vars:
            body = vars_from_var_map + '\n' + body

        if self.method_number != 0:
            body = 'key_stack.pop()\n' + body

        
        res += f'def {class_name_camel_case}_{self.method_name}({method_signature}): \n'
        res += indent(body, '\t')
        return res

    @staticmethod
    def extract_in_vars(in_vars: set[str]):
        return '\n'.join(f'{v} = variable_map[\'{v}\']' for v in in_vars)