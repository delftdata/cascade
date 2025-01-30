from itertools import count

from cascade.frontend.dataflow_analysis.cfg_visiter import CFGVisitor
from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph
from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, Block, SplitBlock, IFBlock

class NameBlocks(CFGVisitor):

    def __init__(self, cfg: ControlFlowGraph, method_name: str):
        self.cfg: ControlFlowGraph = cfg
        self.method_name: str = method_name
        self.counter = count()
        self.if_cond_counter = count()
        self.split_counter = count()

    def name(self):
        self.breadth_first_walk(self.cfg)

    def visit_block(self, block: Block):
        block.name = f'{self.method_name}_{next(self.counter)}'
    
    def visit_ifblock(self, block: IFBlock):
        block.name = f'{self.method_name}_if_cond_{next(self.if_cond_counter)}'
    
    def visit_splitblock(self, block: SplitBlock):
        block.name = f'{self.method_name}_split_{next(self.split_counter)}'
