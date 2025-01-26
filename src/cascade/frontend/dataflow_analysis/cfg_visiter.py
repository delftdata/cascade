from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock


class CFGVisitor:


    def visit_blocks(self, blocks):
        for block in blocks:
                self.visit_generic_block(block)

    def visit_generic_block(self, block: BaseBlock):
        method = "visit_" + block.__class__.__name__.lower()
        visitor = getattr(self, method, self.generic_split)
        visitor(block)
    
    def generic_split(self, block):
        raise Exception(f'Visiter for {type(block)} is not implemented.')
