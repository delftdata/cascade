from klara.core.cfg import RawBasicBlock
from klara.core import nodes


from cascade.frontend. intermediate_representation import Statement

def unparse(block: RawBasicBlock):
    match type(block):
        case nodes.Return:
            return f'return {unparse(block.value)}'
        case nodes.AugAssign:
            return f'{unparse(block.target)} {block.op}= {unparse(block.value)}'
        case nodes.Assign:
            target, *rest = block.targets
            return f'{repr(target)} = {unparse(block.value)}'
        case nodes.Attribute:
            return f'{block.value}.{block.attr}'
        case nodes.Name:
            return repr(block)
        case nodes.BinOp:
            return f'{unparse(block.left)} {block.op} {unparse(block.right)}'
        case _:
            return str(block)
