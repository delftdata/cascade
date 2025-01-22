
from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph
from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, Block, IFBlock, SplitBlock
from cascade.frontend.dataflow_analysis.split_stratagy import SplitStratagy
from cascade.dataflow.dataflow import DataFlow, Edge, OpNode, InvokeMethod


class SplitAnalyzer:
    """ Splits blocks of CFG
    """

    def __init__(self, cfg: ControlFlowGraph, split_stratagy: SplitStratagy):
        self.cfg: ControlFlowGraph = cfg
        self.split_stratagy: SplitStratagy = split_stratagy
        self.new_blocks: list[BaseBlock] = []
    
    def split(self):
        for block in self.cfg.blocks:
            self.split_generic_block(block)
        self.add_new_blocks_to_cfg()
        
    def split_generic_block(self, block: BaseBlock):
        method = "split_" + block.__class__.__name__.lower()
        visitor = getattr(self, method, self.generic_split)
        visitor(block)
    
    def split_ifblock(self, block: IFBlock):
        block.test
        self.split_generic_block(block.body)
        self.split_generic_block(block.or_else)

    def split_block(self, block: Block):
        """ Split block and than adjust edges for the cfg.
            - Replaces "Block" with "SplitBlock" if remote method call is invoked. 
            - Set next node of the splitblock.
            - Update previous links to block to point to new SplitBlock.
            - if block is the body/orelse branch of an IfBlock, then body, orelse respec.
            needs to be replaced with the first split.
        """
        continuation = block.statements
        previous_blocks: list[BaseBlock] = self.cfg.incomming_edges(block)
        first_split: SplitBlock = None
        while self.split_stratagy.contains_remote_entity_invocation(continuation):
            split_block, continuation = self.split_stratagy.split(continuation)
            if not first_split:
                self.cfg.remove_block(block)
                first_split = split_block
            for b in previous_blocks:
                b.set_next_block(split_block)
            self.new_blocks.append(split_block)
            previous_block = split_block
        
        # The pressence of first split indicates that a split occured.
        if first_split:
            # Replace link of incomming edges with the first split.
            for b in self.cfg.incomming_edges(block):
                b.replace_link(first_split, block)

            # Set the next block of the last split replacing the  the next block 
            # of the original block.
            for b in block.get_next_blocks():
                previous_block.set_next_block(b)
    
    def add_new_blocks_to_cfg(self):
        self.cfg.blocks.extend(self.new_blocks)
    
    def generic_split(self, block):
        raise NotImplementedError()