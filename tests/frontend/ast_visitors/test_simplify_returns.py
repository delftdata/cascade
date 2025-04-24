from cascade.frontend.ast_visitors.simplify_returns import SimplifyReturns, simplify_returns
from cascade.frontend.generator.unparser import unparse
from cascade.preprocessing import setup_cfg


def test_simplify_return_state():
    code = "return self.balance"
    cfg, tree = setup_cfg(code, preprocess=False)

    simplify_returns(tree)

    new = [unparse(s) for s in tree.get_statements()]
    assert new == ["__ret_0 = self.balance", "return __ret_0"]
    
        
def test_simplify_return_name():
    code = "return cat"
    cfg, tree = setup_cfg(code, preprocess=False)

    simplify_returns(tree)

    new = [unparse(s) for s in tree.get_statements()]
    assert new == ["return cat"]

def test_simplify_return_binop():
    code = """a = 1
return 4 + 1"""
    cfg, tree = setup_cfg(code, preprocess=False)


    simplify_returns(tree)

    new = [unparse(s) for s in tree.get_statements()]
    assert new == ["a = 1", "__ret_0 = 4 + 1", "return __ret_0"]

def test_simplify_return_multiple():
    code = """a = 1
if a == 1:
    return 3 + 2
else:
    return a"""
    cfg, tree = setup_cfg(code, preprocess=False)

    simplify_returns(tree)

    new = [unparse(s) for s in tree.get_statements()]
    # if statements aren't returned in get_statements
    assert new == ["a = 1", "__ret_0 = 3 + 2", "return __ret_0", "return a"]