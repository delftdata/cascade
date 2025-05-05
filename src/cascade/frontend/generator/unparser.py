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
            return f'{unparse(target)} = {unparse(block.value)}'
        case nodes.Attribute:
            return f'{block.value}.{block.attr}'
        case nodes.AssignName:
            return repr(block)
        case nodes.Name:
            return repr(block)
        case nodes.BinOp:
            return f'{unparse(block.left)} {block.op} {unparse(block.right)}'
        case nodes.Subscript:
            return str(block)
        case nodes.Const:
            return str(block)
        case nodes.NameConstant:
            return str(block)
        case nodes.Compare:
            res = unparse(block.left)
            for op, operand in zip(block.ops, block.comparators):
                res += " {} {}".format(op, unparse(operand))
            return res
        case nodes.Bool:
            return repr(block)
        case nodes.If:
            print(block.test, block.body, block.orelse)
            raise NotImplementedError(type(block), "Should have been removed in previous CFG pass")
        case nodes.FunctionDef:
            return str(block).replace('"', "'")
        case nodes.Call:
            return "{}{}".format(str(block.func), tuple(block.args))
        case nodes.UnaryOp:
            return "{}{}".format(str(block.op), unparse(block.operand))
        case nodes.Expr:
            return unparse(block.value)
        case nodes.BoolOp:
            res = unparse(block.values[0])
            for v in block.values[1:]:
                res += " {} {}".format(block.op, unparse(v))
            return res  
        case nodes.Tuple:
            vals = [unparse(v) for v in block.elts]

            if len(vals) > 1:
                res = f"({','.join(vals)})"
            elif len(vals) == 1:
                res = f"({vals[0]},)"
            else:
                res = "tuple()"
            return res
        
        case nodes.List:
            vals = [unparse(v) for v in block.elts]

            res = f"[{','.join(vals)}]"
            return res
        case nodes.Pass:
            return "pass"
           
        case _:
            raise NotImplementedError(f"{type(block)}: {block}")
