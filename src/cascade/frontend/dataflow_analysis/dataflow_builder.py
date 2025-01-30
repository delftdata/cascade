

from cascade.frontend.dataflow_analysis.cfg_visiter import CFGVisitor
from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph
from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, IFBlock, Block, SplitBlock

class DataflowBuilder(CFGVisitor):

    def __init__(self, cfg: ControlFlowGraph):
        self.cfg: ControlFlowGraph = cfg

    def visit_block(self, block: Block):
        pass

    def visit_ifblock(self, block: IFBlock):
        pass

    def visit_splitblock(self, block: SplitBlock):
        pass
        