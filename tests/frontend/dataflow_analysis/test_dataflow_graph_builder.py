from textwrap import dedent

import networkx as nx 

from klara.core.cfg import Cfg
from klara.core import nodes

from cascade.frontend.dataflow_analysis.dataflow_graph_builder import DataflowGraphBuilder
from cascade.frontend.intermediate_representation import Statement, StatementDataflowGraph
from cascade.frontend.util import setup_cfg


def get_statment(df: StatementDataflowGraph, v: nodes.Statement):
    return next(s for s in df.graph.nodes if s.block == v)


def edge_exists_between(df: StatementDataflowGraph, v: nodes.Statement, n: nodes.Statement):
    statement_v: Statement = get_statment(df, v)
    statement_n: Statement = get_statment(df, n)
    assert (statement_v, statement_n) in df.graph.edges

def assert_expected_edges(df, expected_edges):
    edges: list[nodes.Statement] = [(n.block, v.block) for n,v in df.graph.edges]
    assert edges == expected_edges

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
    cfg: Cfg = setup_cfg(program)
    blocks = cfg.block_list
    user_class: nodes.Block = blocks[2] 
    buy_item: nodes.FunctionDef = user_class.blocks[1].ssa_code.code_list[0]
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
