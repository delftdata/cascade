from cascade.frontend.util import setup_cfg
from cascade.frontend.ast_visitors.variable_getter import VariableGetter

from klara.core.tree_rewriter import AstBuilder


def test_variable_getter():
        code = "item_price = item.get_price()"
        cfg = setup_cfg(code)
        ssa_code = cfg.block_list[1].ssa_code
        node, = ssa_code.code_list 
        variable_getter = VariableGetter.get_variable(node)
        targets_as_string = [repr(t) for t in variable_getter.targets]
        values_as_string = [repr(v) for v in variable_getter.values]
        assert targets_as_string == ['item_price_0']
        assert values_as_string == ['item']
        