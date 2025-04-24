from cascade.preprocessing import setup_cfg
from cascade.frontend.ast_visitors.variable_getter import VariableGetter


def test_variable_getter():
        code = "item_price = item.get_price()"
        cfg, _ = setup_cfg(code)
        ssa_code = cfg.block_list[1].ssa_code
        node, = ssa_code.code_list 
        variable_getter = VariableGetter.get_variable(node)
        targets_as_string = [repr(t) for t in variable_getter.targets]
        values_as_string = [repr(v) for v in variable_getter.values]
        assert targets_as_string == ['item_price_0']
        assert values_as_string == ['item']
        

def test_variable_getter_attr():
        code = "self.balance = self.balance + 1"
        cfg, _ = setup_cfg(code, preprocess=False)
        ssa_code = cfg.block_list[1].ssa_code
        node, = ssa_code.code_list 
        variable_getter = VariableGetter.get_variable(node)
        targets_as_string = [repr(t) for t in variable_getter.targets]
        values_as_string = [repr(v) for v in variable_getter.values]
        assert targets_as_string == ['self']
        assert values_as_string == ['self']