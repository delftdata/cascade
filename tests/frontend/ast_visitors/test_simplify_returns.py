from cascade.frontend.ast_visitors.simplify_returns import SimplifyReturns, simplify_returns
from cascade.frontend.generator.unparser import unparse
from cascade.preprocessing import setup_cfg
from klara.core import nodes
from klara.core.tree_rewriter import AstBuilder
from klara.core.cfg import Cfg

def setup_cfg_no_ssa(code: str) -> Cfg:
    as_tree = AstBuilder().string_build(code)
    cfg = Cfg(as_tree)
    return cfg, as_tree

def test_simplify_return_state():
    code = "return self.balance"
    cfg, tree = setup_cfg_no_ssa(code)
    for s in tree.get_statements():
        print(repr(s))
    sr = SimplifyReturns.replace(tree)
    simplify_returns(tree)

    for s in tree.get_statements():
        print(repr(s))
        
def test_simplify_return_name():
    code = "return cat"
    cfg, tree = setup_cfg_no_ssa(code)
    for s in tree.get_statements():
        print(repr(s))
    sr = SimplifyReturns.replace(tree)
    simplify_returns(tree)

    for s in tree.get_statements():
        print(repr(s))

def test_simplify_return_binop():
    code = """a = 1
return 4+1"""
    cfg, tree = setup_cfg_no_ssa(code)

    for s in tree.get_statements():
        print(repr(s))
    simplify_returns(tree)

    for s in tree.get_statements():
        print(repr(s))

def test_simplify_return_multiple():
    code = """a = 1
if a == 1:
    return 3 + 2
else:
    return a"""
    cfg, tree = setup_cfg_no_ssa(code)

    for b in tree.get_statements():
        print(repr(b))
    simplify_returns(tree)

    for b in tree.get_statements():
        print(repr(b))