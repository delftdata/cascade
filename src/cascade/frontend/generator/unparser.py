from klara.core.cfg import RawBasicBlock
from klara.core import nodes


from cascade.frontend. intermediate_representation import Statement


class Unparser:

    @staticmethod
    def unparse(statements: list[Statement]):
        statements_as_string = []
        for s in statements:
            block: RawBasicBlock = s.block
            if type(block) == nodes.FunctionDef:
                continue
            statements_as_string.append(str(block))
        return '\n'.join(statements_as_string)
