from dataclasses import dataclass


from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock

@dataclass
class ControlFlowGraph:
    blocks: list[BaseBlock]
    entry: BaseBlock

    def incomming_edges(self, block: BaseBlock) -> list[BaseBlock]:
        return [b for b in self.blocks if block in b.get_next_blocks()]
    
    def remove_block(self, block: BaseBlock):
        self.blocks.remove(block)
