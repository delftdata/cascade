
from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph
from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, Block, IFBlock, SplitBlock
from cascade.frontend.dataflow_analysis.split_stratagy import SplitStratagy
from cascade.frontend.dataflow_analysis.cfg_visiter import CFGVisitor



class SplitAnalyzer(CFGVisitor):
    """ Splits blocks of CFG
    """

    def __init__(self, cfg: ControlFlowGraph, split_stratagy: SplitStratagy):
        self.cfg: ControlFlowGraph = cfg
        self.split_stratagy: SplitStratagy = split_stratagy
        self.new_blocks: list[BaseBlock] = []
    
    def split(self):
        self.breadth_first_walk(self.cfg)
        self.add_new_blocks_to_cfg()

    def visit_block(self, block: Block):
        """ Split block and than adjust edges for the cfg.
            - Replaces "Block" with "SplitBlock" if remote method call is invoked. 
            - Set next node of the splitblock.
            - Update previous links to block to point to new SplitBlock.
            - if block is the body/orelse branch of an IfBlock, then body, orelse respec.
            needs to be replaced with the first split.
        """
        continuation = block.statements
        first_split: SplitBlock = None
        previous_block: BaseBlock = None
        while self.split_stratagy.contains_remote_entity_invocation(continuation):
            split_block, continuation = self.split_stratagy.split(continuation)
            if not first_split:
                self.cfg.remove_block(block)
                # Update the CFG entry if block is current entry.
                if self.cfg.entry == block:
                    self.cfg.entry = split_block
                first_split = split_block

            # In the case of multiple splits set the next block of the previously created
            # splitblock to the newly created split block. 
            if previous_block:
                previous_block.set_next_block(split_block)
            previous_block = split_block
            self.new_blocks.append(split_block)
        
        # Create a block from the left over continuation statements if any.
        # If not "previous_block" there was no split and there is no need to
        # create a block of contiunation since "block" was never removed from cfg.blocks. 
        if continuation and previous_block:
            continuation_block: Block = Block(continuation)
            previous_block.set_next_block(continuation_block)
            previous_block = continuation_block
            self.new_blocks.append(continuation_block)
        
        # The existence of first split indicates that a split occured.
        if first_split:
            # Replace link of incomming edges with the first split.
            # What if the incomming edges are body and orelse of an if statement.
            for b in self.cfg.incomming_edges(block):
                b.replace_link(first_split, block)

            # Set the next block of the last split replacing the next block 
            # of the original block.
            previous_block.set_next_block(block.next_block)

    
    def add_new_blocks_to_cfg(self):
        self.cfg.blocks.extend(self.new_blocks)
