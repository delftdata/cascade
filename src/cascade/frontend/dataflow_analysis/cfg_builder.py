import ast

from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, Block, IFBlock
from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph


class CFGBuilder(ast.NodeVisitor):

    def __init__(self):
        self.blocks = []
        self.statments: list[ast.stmt] = []
        self.previous_block: BaseBlock = None
        self.entry: BaseBlock = None

    def visit_statement(self, node: ast.stmt):
        """ Add statement to cfg
        """
        self.statments.append(node)
    
    def create_block(self):
        if not self.statments:
            return
        block: Block = Block(self.statments)
        self.add_block(block)
        self.statments = []
    
    def visit_If(self, node: ast.If):
        """ Add if statement to cfg
        """
        self.create_block()
        if_cfg_builder: CFGBuilder = CFGBuilder.build(node.body)
        else_cfg_builder: CFGBuilder = CFGBuilder.build(node.orelse)
        if_block: IFBlock = IFBlock(node.test, if_cfg_builder.entry, else_cfg_builder.entry)
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
    def build(cls, statements: list[ast.stmt], previous_block=None) -> "CFGBuilder":
        builder = cls()
        if previous_block:
            builder.set_previous_block(previous_block)
        builder.visit_list(statements)
        builder.create_block()
        return builder
    
    @staticmethod
    def build_cfg(statements: list[ast.stmt]) -> ControlFlowGraph:
        builder = CFGBuilder.build(statements)
        assert builder.blocks and builder.entry, f"Control flow graph lacks an entry node ({builder.entry}), and/or blocks: {builder.blocks}"
        return ControlFlowGraph(builder.blocks, builder.entry)

    def generic_visit(self, node):
        if isinstance(node, ast.stmt):
            self.visit_statement(node)
        super().generic_visit(node)
