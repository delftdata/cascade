from cascade.frontend.intermediate_representation import StatementDataflowGraph
from cascade.frontend.generator.compiler_pass_3 import BuildCompiledMethodsString
from textwrap import dedent
from cascade.frontend.util import setup_cfg
from cascade.frontend.dataflow_analysis.class_list_builder import ClassListBuilder
from cascade.frontend.dataflow_analysis.class_wrapper import ClassWrapper
from cascade.frontend.dataflow_analysis.class_list import ClassList
from cascade.frontend.generator.generate_split_functions import GenerateSplittFunctions
from cascade.frontend.generator.generate_dataflow import GenerateDataflow
from cascade.dataflow.dataflow import DataFlow


def test_simple():

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
    class_name: str = 'User'
    entity_1: ClassWrapper = class_list.get_class_by_name(class_name)
    dataflow_graph: StatementDataflowGraph = entity_1.methods['buy_item']
    split_functions = GenerateSplittFunctions.generate(dataflow_graph, class_name)
    df: DataFlow = GenerateDataflow.generate(split_functions, dataflow_graph.instance_type_map)
    print('dataflow nodes:')
    for n in df.nodes.values():
        print(n)

    print(df.adjacency_list)

    compiled_methods: str = BuildCompiledMethodsString.build(split_functions)
    print(compiled_methods)
    
    print("-"*100)
    print('ITEM: ')
    class_name: str = 'Item'
    entity_1: ClassWrapper = class_list.get_class_by_name(class_name)
    dataflow_graph: StatementDataflowGraph = entity_1.methods['get_price']
    split_functions = GenerateSplittFunctions.generate(dataflow_graph, class_name)
    df: DataFlow = GenerateDataflow.generate(split_functions, dataflow_graph.instance_type_map)
    print('dataflow nodes:')
    for n in df.nodes.values():
        print(n)

    print(df.adjacency_list)

    compiled_methods: str = BuildCompiledMethodsString.build(split_functions)
    print(compiled_methods)
    assert False

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
