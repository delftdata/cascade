import copy 

from klara.core import nodes

from cascade.descriptors.split_descriptor import SplitDescriptor
from cascade.frontend.ast_visitors import VariableGetter

class SplitControlFlow:
    
    def __init__(self):
        self.splits: list[SplitDescriptor] = []
    
    def add_split(self, split: SplitDescriptor):
        self.splits.append(split)

    def split_function_body(self, body: list[str], method_name: str, live_variables: list):
        live_variables = copy.copy(live_variables)
        new_live_variables = []
        i = 0
        split = []
        for node in body:
            match type(node):
                case nodes.If: 
                    if split:
                        split_desciptor: SplitDescriptor = SplitDescriptor(method_name, split, live_variables)
                        self.add_split(split_desciptor)
                    if_cond_node: nodes.Return = nodes.Return()
                    if_cond_node.postinit(value=node.test)
                    if_cond_split: SplitDescriptor = SplitDescriptor(method_name + '_if_cond', [if_cond_node], live_variables)
                    self.add_split(if_cond_split)
                    self.split_function_body(node.body, method_name + '_body', new_live_variables + live_variables)
                    self.split_function_body(node.orelse, method_name + '_orelse', new_live_variables + live_variables)
                    split = []
                    i += 1
                case _:
                    split.append(node)
            variable_getter = VariableGetter.get_variable(node)
            new_live_variables.extend(variable_getter.targets)
        if split:
            split_desciptor: SplitDescriptor = SplitDescriptor(method_name, split, live_variables + new_live_variables)
            self.add_split(split_desciptor)


    @classmethod
    def split(cls, function_def: nodes.FunctionDef, method_name: str) -> list[SplitDescriptor]:
        c = cls()
        c.split_function_body(function_def.body, method_name, copy.copy(function_def.args.args))
        return c.splits

