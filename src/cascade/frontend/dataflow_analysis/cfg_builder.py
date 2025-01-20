
from klara.core.ssa_visitors import AstVisitor
from klara.core import nodes

from cascade.descriptors.method_descriptor import MethodDescriptor
from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, Block, IFBlock


class CFGBuiler(AstVisitor):

    def __init__(self):
        self.blocks = []
        self.statments: list[nodes.Statement] = []
        self.previous_block: BaseBlock = None
        self.entry: BaseBlock = None

    def visit_statement(self, node: nodes.Statement):
        """ Add statement to cfg
        """
        self.statments.append(node)
    
    def create_block(self):
        if not self.statments:
            return
        block: Block = Block(self.statments)
        self.add_block(block)
        self.statments = []
    
    def visit_if(self, nodes: nodes.If):
        """ Add if statement to cfg
        """
        self.create_block()
        if_cfg_builder: CFGBuiler = CFGBuiler.build(nodes.body)
        else_cfg_builder: CFGBuiler = CFGBuiler.build(nodes.orelse)
        if_block: IFBlock = IFBlock(nodes.test, if_cfg_builder.entry, else_cfg_builder.entry)
        self.add_block(if_block)
        self.blocks.extend(if_cfg_builder.blocks)
        self.blocks.extend(else_cfg_builder.blocks)
    
    def add_block(self, block: BaseBlock):
        self.blocks.append(block)
        self.set_previous_block(block)
        self.set_entry_if_not_exists(block)
    
    def set_entry_if_not_exists(self, block: BaseBlock):
        if not self.entry:
            self.entry = block

    def set_previous_block(self, block: Block):
        if self.previous_block:
            self.previous_block.set_next_block(block)
        self.previous_block = block

    def visit_list(self, statements):
        for s in statements:
            self.visit(s)

    @classmethod
    def build(cls, statements: list[nodes.Statement], previous_block=None):
        builder = cls()
        if previous_block:
            builder.set_previous_block(previous_block)
        builder.visit_list(statements)
        builder.create_block()
        return builder


    def generic_visit(self, node):
        if isinstance(node, nodes.Statement):
            self.visit_statement(node)
        super().generic_visit(node)
