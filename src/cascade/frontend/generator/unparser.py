from klara.core.cfg import RawBasicBlock
from klara.core import nodes


def unparse(block: RawBasicBlock):
    match type(block):
        case nodes.Return:
            return f'return {unparse(block.value)}'
        case nodes.AugAssign:
            raise NotImplementedError()
            # TODO: augassign does not work well with ssa
            # e.g. 
            # a = 1
            # a += 2
            # will generate:
            # a_0 = 1
            # a_1 += 2
            # The last line should be desugared into
            # a_1 = a_0 + 2 (perhapse with a n Ast.Visitor?)
            return f'{repr(block.target)} {block.op}= {unparse(block.value)}'
        case nodes.Assign:
            target, *rest = block.targets
            return f'{repr(target)} = {unparse(block.value)}'
        case nodes.Attribute:
            return f'{block.value}.{block.attr}'
        case nodes.Name:
            return repr(block)
        case nodes.BinOp:
            return f'{unparse(block.left)} {block.op} {unparse(block.right)}'
        case nodes.Subscript:
            return str(block)
        case nodes.Const:
            return str(block)
        case nodes.Compare:
            return str(block)
        case nodes.Bool:
            return repr(block)
        case nodes.If:
            print(block.test, block.body, block.orelse)
            raise NotImplementedError(type(block), "Should have been removed in previous CFG pass")
        case nodes.FunctionDef:
            return str(block).replace('"', "'")
        case _:
            return str(block)
