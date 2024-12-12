import networkx as nx


from klara.core import nodes
from klara.core.cfg import FunctionLabel, Cfg, ModuleLabel, TempAssignBlock
from klara.core.nodes import Name, FunctionDef
from klara.core.ssa_visitors import VariableGetter
from klara.core.ssa import SsaCode

from cascade.frontend.intermediate_representation import Statement, StatementDataflowGraph
from cascade.frontend.ast_visitors import ContainsAttributeVisitor, ExtractEntityVisitor
from cascade.frontend.dataflow_analysis.dataflow_graph_build_context import DataflowGraphBuildContext
from cascade.frontend.util import setup_cfg


class DataflowGraphBuilder:

    def __init__(self, block_list: list, build_context: DataflowGraphBuildContext):
        self.block_list: list = block_list
        self.build_context: DataflowGraphBuildContext = build_context
        self.color: int = 2 # self is alsways annotated with 1.
        self.entity_map: dict[str, int] = {} # e.g. item_1 -> 2

    def extract_statment_list(self):
        statements = []
        i = 0
        for b in self.block_list:
            if type(b) in [ModuleLabel, TempAssignBlock]:
                continue
            elif type(b) == FunctionDef:
                b: FunctionDef
                statement = Statement(i, b)
                i += 1
                args = b.args
                function_vars = [Name.quick_build(f'{a.arg}_0') for a in args.args]
                statement.extend_targets(function_vars)
                statement.extend_values(function_vars)
                statements.append(statement)
            else:
                statement = Statement(i, b)
                i += 1
                statements.append(statement)
                vars = VariableGetter.get_variable(b)
                statement.extend_targets(vars.targets)
                statement.extend_values(vars.values)
                contains_attribute, name = ContainsAttributeVisitor.check(b)
                if contains_attribute:
                    if name == 'self':
                        color: int = self.get_scope_color()
                        statement.set_color(color)
                    else:
                        color: int = self.get_color_for_var_name(name)
                        statement.set_color(color)
                        statement.set_remote()

                    statement.set_attribute_name(name)
        return statements
    
    def get_color_for_var_name(self, name: str) -> int:
        if name in self.entity_map:
            return self.entity_map[name]
        if self.build_context.is_entity(name):
            color = self.color
            self.update_entity_map(name, color)
            self.color += 1
            return color
        return 0
    
    def update_entity_map(self, name: str, color: int):
        self.entity_map[name] = color
    
    def get_color_type_map(self) -> dict[int, str]:
        buildContext: StatementDataflowGraph = self.build_context
        color_type_map: dict[int, str] = {}
        for name, color in self.entity_map.items():
            entity_name = buildContext.get_entity_for_var_name(name)
            color_type_map[color] = entity_name
        return color_type_map


    def get_scope_color(self):
        """ Scope self should always have color 1
        """
        return 1
    
    def construct_dataflow_graph(self) -> StatementDataflowGraph:
        statements = self.extract_statment_list()
        G = nx.DiGraph()
        for b1 in statements:
            G.add_node(b1)
            for b2 in statements:
                if b1.block_num != b2.block_num:
                    targets = set(repr(b) for b in b1.targets)
                    values = set(repr(b) for b in b2.values)
                    if targets.intersection(values):
                        G.add_edge(b1, b2)
        return StatementDataflowGraph(G, self.get_color_type_map())

    @classmethod
    def build(cls, block_list: list, build_context: DataflowGraphBuildContext) -> StatementDataflowGraph:
        dataflow_graph_builder = cls(block_list, build_context)
        return dataflow_graph_builder.construct_dataflow_graph()
