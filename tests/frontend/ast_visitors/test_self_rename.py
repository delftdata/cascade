from cascade.frontend.ast_visitors.replace_name import ReplaceSelfWithState
from cascade.frontend.util import setup_cfg
from cascade.frontend.ast_visitors.variable_getter import VariableGetter
from klara.core import nodes

def test_replace_self_with_state():
    code = "self.balance = self.balance + 10"
    cfg = setup_cfg(code)
    ssa_code = cfg.block_list[1].ssa_code
    node, = ssa_code.code_list 
    ReplaceSelfWithState.replace(node)

    assert isinstance(node, nodes.Assign)
    assert isinstance(node.targets, list)
    assert isinstance(node.value, nodes.BinOp)
    assert str(node.targets[0]) == "state['balance']"
    assert str(node.value.left) == "state['balance']"
        
def test_replace_self_with_state_dict():
    code = "self.data['b'] = self.data['a'] + self.balance"
    cfg = setup_cfg(code)
    ssa_code = cfg.block_list[1].ssa_code
    node, = ssa_code.code_list 
    ReplaceSelfWithState.replace(node)

    assert isinstance(node, nodes.Assign)
    assert isinstance(node.targets, list)
    assert isinstance(node.value, nodes.BinOp)
    assert str(node.targets[0]) == "state['data']['b']"
    assert str(node.value.left) == "state['data']['a']"
    assert str(node.value.right) == "state['balance']"