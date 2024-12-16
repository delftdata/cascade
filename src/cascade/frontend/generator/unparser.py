from klara.core.cfg import RawBasicBlock
from klara.core import nodes


from cascade.frontend. intermediate_representation import Statement


class Unparser:

    @staticmethod
    def unparse(statements: list[Statement]):
        statements_as_string = []
        for s in statements:
            block: RawBasicBlock = s.block
            match type(block):
                case nodes.Return:
                    block: nodes.Return
                    string: str = f'return {block.value}'
                case nodes.AugAssign:
                    block: nodes.AugAssign
                    string: str = f'{repr(block.target)} {block.op}= {block.value}'
                case nodes.Assign:
                    block: nodes.Assign
                    target, *rest = block.targets
                    string: str = f'{repr(target)} = {block.value}'
                case nodes.FunctionDef:
                    continue
                case _:
                    string =  str(block)
            statements_as_string.append(string)
        return '\n'.join(statements_as_string)
