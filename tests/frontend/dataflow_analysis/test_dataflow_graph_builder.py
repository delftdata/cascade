from textwrap import dedent

import networkx as nx 

from klara.core.cfg import Cfg
from klara.core import nodes

from cascade.frontend.dataflow_analysis.dataflow_graph_builder import DataflowGraphBuilder
from cascade.frontend.intermediate_representation import Statement, StatementDataflowGraph
from cascade.frontend.util import setup_cfg


def assert_expected_edges(df, expected_edges):
    edges: list[nodes.Statement] = [(n.block, v.block) for n,v in df.graph.edges]
    assert edges == expected_edges


def setup_test(program: str) -> nodes.FunctionDef:
    cfg: Cfg = setup_cfg(program)
    blocks = cfg.block_list
    user_class: nodes.Block = blocks[2] 
    f = user_class.blocks[1].ssa_code.code_list[0]
    return f

# TODO: FOr instance in the example below there is a indirect dependency between update balence and 
# returning the balence >= 0. (side effect dependency)
def test_simple_dataflow_graph():
    program: str = dedent("""
                class User:
                    
                    def buy_item(self, item: 'Item') -> bool:
                        item_price = item.get_price()
                        self.balance -= item_price
                        return self.balance >= 0
                        """)
    buy_item: nodes.FunctionDef = setup_test(program)
    buy_item_body_0 = buy_item.body[0]
    buy_item_body_1 = buy_item.body[1]
    buy_item_body_2 = buy_item.body[2]
    df: StatementDataflowGraph = DataflowGraphBuilder.build([buy_item] + buy_item.body)
    expected_edges = [
        (buy_item, buy_item_body_0),
        (buy_item, buy_item_body_1),
        (buy_item, buy_item_body_2),
        (buy_item_body_0, buy_item_body_1)
    ]
    assert_expected_edges(df, expected_edges)


def test_buy_two_items_dataflow():
    program: str = dedent("""
                class User:
                    
                    def buy_item(self, item_0: 'Item', item_1: 'Item') -> bool:
                        discount = 10
                        item_price_0 = item_0.get_price(discount)
                        item_price_1 = item_1.get_price(discount)
                        total_price = item_price_0 + item_price_1
                        self.balance -= total_price
                        return self.balance >= 0
                        """)
    buy_item: nodes.FunctionDef = setup_test(program)
    buy_item_body_0 = buy_item.body[0]
    buy_item_body_1 = buy_item.body[1]
    buy_item_body_2 = buy_item.body[2]
    df: StatementDataflowGraph = DataflowGraphBuilder.build([buy_item] + buy_item.body)
    expected_edges = [
        (buy_item, buy_item_body_0),
        (buy_item, buy_item_body_1),
        (buy_item, buy_item_body_2),
        (buy_item_body_0, buy_item_body_1)
    ]
    assert_expected_edges(df, expected_edges) 


def test_dataflow_graph_builder_with_if_statement():
    """ We can safely assume that the if statement does not contain any remote entity calls.
        Preprocessing should modify the code such that the remote call to the remote entity is invoked
        before the if statement.
    """
    program: str = dedent("""
            class User:
                
                def buy_item(self, item: 'Item') -> bool:
                    item_price = item.get_price() # body[0]
                    if item_price < 100: # if_stmt_test
                        total_price = item_price + 10 # if_body
                    else:
                        total_price = item_price # or_else
                    self.balance -= total_price # body[2]
                    return self.balance >= 0 # body[3]
                    """) 
    buy_item: nodes.FunctionDef = setup_test(program)
    body = buy_item.body
    if_stmt = body[1]
    if_stmt_test = if_stmt.test
    if_body, = if_stmt.body
    or_else, = if_stmt.orelse
    df: StatementDataflowGraph = DataflowGraphBuilder.build([buy_item] + buy_item.body)
    expected_edges = [
        (buy_item, body[0]),
        (buy_item, body[2]),
        (buy_item, body[3]),
        (body[0], if_stmt_test),
        (body[0], if_body),
        (body[0], or_else),
        (if_stmt_test, if_body),
        (if_stmt_test, or_else),
        (if_body, body[2]),
        (or_else, body[2])
    ]
    assert_expected_edges(df, expected_edges)


def test_dataflow_graph_builder_with_if_statement_containing_remote_entity_invocation():
    """ In this case a remote enitity is invoked in the if branch.
    """
    program: str = dedent("""
            class User:
                
                def buy_item(self, item: 'Item', delivery_service: 'DeliveryService') -> bool:
                    item_price = item.get_price()
                    if item_price < 100:
                        delivery_costs = delivery_service.get_delivery_costs(item_price)
                        total_price = item_price + delivery_costs
                    else:
                        total_price = item_price
                    self.balance -= total_price
                    return self.balance >= 0
                    """) 
    buy_item: nodes.FunctionDef = setup_test(program)