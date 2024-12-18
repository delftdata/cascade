from klara.core.cfg import RawBasicBlock
from klara.core import nodes


from cascade.frontend. intermediate_representation import Statement


class Unparser:

    @staticmethod
    def unparse(statements: list[Statement]):
        statements_as_string = []
        for s in statements:
            statements_as_string.append(Unparser.unparse_block(s.block))
        return '\n'.join(statements_as_string)
    
    @staticmethod
    def unparse_block(block: RawBasicBlock):
        match type(block):
            case nodes.Return:
                block: nodes.Return
                string: str = f'return {block.value}'
            case nodes.AugAssign:
                block: nodes.AugAssign
                string: str = f'{repr(block.target)} {block.op}= {repr(block.value)}'
            case nodes.Assign:
                block: nodes.Assign
                target, *rest = block.targets
                string: str = f'{repr(target)} = {repr(block.value)}'
            # case nodes.FunctionDef:
            case _:
                string = str(block)
        return string
