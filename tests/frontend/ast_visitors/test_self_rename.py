from cascade.frontend.ast_visitors.replace_name import ReplaceSelfWithState
from cascade.preprocessing import setup_cfg
from klara.core import nodes

def test_replace_self_with_state():
    code = "self.balance = self.balance + 10"
    cfg, tree = setup_cfg(code, preprocess=False)
    ReplaceSelfWithState.replace(tree)

    assert isinstance(tree, nodes.Module)
    node = tree.body[0]
    assert isinstance(node, nodes.Assign)
    assert isinstance(node.targets, list)
    assert isinstance(node.value, nodes.BinOp)
    assert str(node.targets[0]) == "__state['balance']"
    assert str(node.value.left) == "__state['balance']"
        
def test_replace_self_with_state_dict():
    code = "self.data['b'] = self.data['a'] + self.balance"
    cfg, tree = setup_cfg(code, preprocess=False)
    ReplaceSelfWithState.replace(tree)


    assert isinstance(tree, nodes.Module)
    node = tree.body[0]
    assert isinstance(node, nodes.Assign)
    assert isinstance(node.targets, list)
    assert isinstance(node.value, nodes.BinOp)
    assert str(node.targets[0]) == "__state['data']['b']"
    assert str(node.value.left) == "__state['data']['a']"
    assert str(node.value.right) == "__state['balance']"

def test_replace_self_assign():
    code = "__ret_2 = self.price"
    cfg, tree = setup_cfg(code, preprocess=False)
    ReplaceSelfWithState.replace(tree)


    assert isinstance(tree, nodes.Module)
    node = tree.body[0]
    assert isinstance(node, nodes.Assign)
    assert isinstance(node.targets, list)
    assert isinstance(node.value, nodes.Subscript), type(node.value)
    assert str(node.targets[0]) == "__ret_2"
    assert str(node.value) == "__state['price']"
    print(str(node))

def test_replace_self_assign_after_return():
    code = "__ret_2 = self.price"
    cfg, tree = setup_cfg(code, preprocess=False)
    ReplaceSelfWithState.replace(tree)

    assert isinstance(tree, nodes.Module)
    node = tree.body[0]
    assert isinstance(node, nodes.Assign)
    assert isinstance(node.targets, list)
    assert isinstance(node.value, nodes.Subscript), type(node.value)
    assert str(node.targets[0]) == "__ret_2"
    assert str(node.value) == "__state['price']"
    print(str(node))