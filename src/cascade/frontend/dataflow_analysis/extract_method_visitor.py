from klara.core.ssa_visitors import AstVisitor
from klara.core.nodes import FunctionDef 

from cascade.frontend.dataflow_analysis.dataflow_graph_builder import DataflowGraphBuilder
from cascade.frontend.dataflow_analysis.dataflow_graph_build_context import DataflowGraphBuildContext
from cascade.frontend.intermediate_representation import StatementDataflowGraph


class ExtractMethodVisitor(AstVisitor):

    def __init__(self, build_context: DataflowGraphBuildContext):
        self.build_context: DataflowGraphBuildContext = build_context
        self.methods: dict[str, StatementDataflowGraph] = {}
    
    def visit_functiondef(self, node: FunctionDef):
        name: str = str(node.name)
        statements = [node] + node.body
        assert name not in self.methods, "A method should be only defined once"
        dataflow_graph: StatementDataflowGraph = DataflowGraphBuilder.build(statements, self.build_context)
        dataflow_graph.set_name(name)
        self.methods[name] = dataflow_graph
    
    def visit_Function(self, node):
        print('visiting funciton node')

    @classmethod
    def extract(cls, node, build_context: DataflowGraphBuildContext):
        """Node should be a top level class node"""
        c = cls(build_context)
        c.visit(node)
        return c.methods
