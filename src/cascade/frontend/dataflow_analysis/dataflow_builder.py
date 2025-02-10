import ast


from cascade.dataflow.dataflow import DataFlow, Edge, InvokeMethod, OpNode
from cascade.dataflow.operator import StatefulOperator
from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph
from cascade.frontend.dataflow_analysis.cfg_visiter import CFGVisitor
from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, IFBlock, Block, SplitBlock
from cascade.frontend.ast_visitors import ContainsAttributeVisitor, VariableGetter


def build_dataflow(df_name: str, cfg: ControlFlowGraph, self_operator: StatefulOperator, operators: dict[str, StatefulOperator], instance_type_map: dict[str, str]):
    node_builder = BuildNodes(cfg, self_operator, operators, instance_type_map)
    nodes = node_builder.build()
    df = DataFlow(df_name)
    edge_builder = BuildEdges(cfg, self_operator, nodes, df, operators)
    edge_builder.build()
    return df


class BuildNodes(CFGVisitor):

    def __init__(self, cfg: ControlFlowGraph, self_operator: StatefulOperator, operators: dict[str, StatefulOperator], instance_type_map: dict[str, str]): 
        self.cfg = cfg
        self.instance_type_map: dict[str, str] = instance_type_map
        self.stateful_operator: StatefulOperator = self_operator
        self.operators: dict[str, StatefulOperator] = operators
        self.nodes = {}

    def build(self):
        self.breadth_first_walk(self.cfg)
        return self.nodes
    
    def visit_block(self, block: Block):
        node: OpNode = OpNode(self.stateful_operator, InvokeMethod(block.name))
        self.nodes[block.name] = node

    def visit_ifblock(self, block: IFBlock):
        node: OpNode = OpNode(self.stateful_operator, InvokeMethod(block.name))
        self.nodes[block.name] = node

    def visit_splitblock(self, block: SplitBlock):
        node: OpNode = OpNode(self.stateful_operator, InvokeMethod(block.name))
        self.nodes[block.name] = node
        self.add_remote_entity_call_node(block.remote_function_calls)

    def add_remote_entity_call_node(self, remote_function_calls: list[ast.stmt]):
        """ Add a node to df that invoces a remote method call.
        """
        for call in remote_function_calls:
            contains_attribute, attribute = ContainsAttributeVisitor.check_and_return_attribute(call)
            variable_getter: VariableGetter = VariableGetter.get_variable(call)
            targets = variable_getter.targets
            assert contains_attribute, 'Remote function call is expected to contain an attribute'
            assert len(targets) == 1, 'Only support statements with one target'
            target, = targets
            value: str = attribute.value.id
            method_name = attribute.attr
            remote_entity_op: StatefulOperator = self.get_operator_from_value(value)
            assign_result_to: str = target.id # extract target with visitor.. 
            remote_node = OpNode(remote_entity_op, InvokeMethod(method_name), assign_result_to=assign_result_to)
            self.nodes[f'{method_name}_remote'] = remote_node
        
    def get_operator_from_value(self, attr: str):
        """ Returns the operator corresponding to the attribute
        """
        operator_name: str = self.instance_type_map[attr]
        return self.operators[operator_name]


class BuildEdges(CFGVisitor):

    def __init__(self, cfg: ControlFlowGraph, self_operator: StatefulOperator, nodes: dict[str, OpNode], df: DataFlow, operators: dict[str, StatefulOperator]):
        self.cfg = cfg
        self.stateful_operator: StatefulOperator = self_operator
        self.nodes = nodes
        self.operators: dict[str, StatefulOperator] = operators
        self.df: DataFlow = df
    
    def build(self):
        self.breadth_first_walk(self.cfg)

    def visit_block(self, block: Block):
        n: OpNode = self.nodes[block.name]
        for b in block.get_next_blocks():
            if b == None:
                continue
            v: OpNode = self.nodes[b.name]
            self.df.add_edge(Edge(n, v))

    def visit_ifblock(self, block: IFBlock):
        n = self.nodes[block.name]
        if_branch_node: OpNode = self.nodes[block.body.name]
        else_branch_node: OpNode = self.nodes[block.or_else.name]
        self.df.add_edge(Edge(n, if_branch_node, if_conditional=True))
        self.df.add_edge(Edge(n, else_branch_node, if_conditional=False))

    def visit_splitblock(self, block: SplitBlock):
        """ Split block invokes remote entity. 
            Add an edge from split function to remote entity operator.
            And from remote entity to next block.
        """
        n: OpNode = self.nodes[block.name]
        assert len(block.remote_function_calls) == 1, 'only suport one remote function call yet.'
        call, = block.remote_function_calls
        contains_attribute, attribute = ContainsAttributeVisitor.check_and_return_attribute(call)
        variable_getter: VariableGetter = VariableGetter.get_variable(call)
        targets = variable_getter.targets
        assert contains_attribute, 'Remote function call is expected to contain an attribute'
        assert len(targets) == 1, 'Only support statements with one target'
        method_name = attribute.attr
        remote_node = self.nodes[f'{method_name}_remote'] 
        self.df.add_edge(Edge(n, remote_node))
        
        # add an edge from remote node to next node
        next_block: BaseBlock = block.next_block
        next_node_name: str = next_block.name
        next_node = self.nodes[next_node_name]
        self.df.add_edge(Edge(remote_node, next_node))


