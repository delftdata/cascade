from cascade.frontend.intermediate_representation import Block, Statement, StatementDataflowGraph
from cascade.frontend.generator.compiler_pass_2 import CompilerPass2
import pytest

@pytest.mark.compiler
def test_simple():
    from textwrap import dedent
    from cascade.frontend.util import setup_cfg, plot_graph_with_color, plot_dataflow_graph
    from cascade.frontend.dataflow_analysis.class_list_builder import ClassListBuilder
    from cascade.frontend.dataflow_analysis.class_wrapper import ClassWrapper
    from cascade.frontend.dataflow_analysis.class_list import ClassList
    from cascade.frontend.generator.generate_split_functions import GenerateSplittFunctions

    example = """\
            class User:
                def __init__(self, key: str, balance: int):
                    self.key: str = key
                    self.balance: int = balance
                
                def buy_item(self, item: 'Item') -> bool:
                    item_price = item.get_price() # SSA
                    self.balance -= item_price
                    return self.balance >= 0
        
            class Item:
                def __init__(self, key: str, price: int):
                    self.key: str = key
                    self.price: int = price

                def get_price(self) -> int:
                    return self.price
            """
    example = dedent(example)
    cfg = setup_cfg(example)
    class_list: ClassList = ClassListBuilder.build(cfg)
    entity_1: ClassWrapper = class_list.get_class_by_name('User')
    dataflow_graph: StatementDataflowGraph = entity_1.methods['buy_item']
    split_functions = GenerateSplittFunctions.generate(dataflow_graph)
    cp2 = CompilerPass2(split_functions)
    body = cp2.make_splitfunctions()
    for b in body:
        print('\nsplit:')
        for b_ in b:
            print(b_)

# """
# def get_price_compiled(variable_map: dict[str, Any], state: Item, key_stack: list[str]) -> Any:
#     key_stack.pop() # final function
#     return state.price

# def buy_item_0_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
#     key_stack.append(variable_map["item_key"])
#     return None

# def buy_item_1_compiled(variable_map: dict[str, Any], state: User, key_stack: list[str]) -> Any:
#     key_stack.pop()
#     state.balance = state.balance - variable_map["item_price"]
#     return state.balance >= 0
# """

# user_op = StatefulOperator(
#     User, 
#     {
#         "buy_item_0": buy_item_0_compiled,
#         "buy_item_1": buy_item_1_compiled,
#     }, 
#     {
#         "buy_item": user_buy_item_df()
#     })

# def user_buy_item_df():
#     df = DataFlow("user.buy_item")
#     n0 = OpNode(User, InvokeMethod("buy_item_0"))
#     n1 = OpNode(Item, InvokeMethod("get_price"), assign_result_to="item_price")
#     n2 = OpNode(User, InvokeMethod("buy_item_1"))
#     df.add_edge(Edge(n0, n1))
#     df.add_edge(Edge(n1, n2))
#     df.entry = n0
#     return df

if __name__ == '__main__':
    test_simple()


