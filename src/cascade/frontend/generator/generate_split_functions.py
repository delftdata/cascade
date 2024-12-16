from itertools import count, chain

import networkx as nx

from cascade.frontend.intermediate_representation import Block, Statement, DataflowGraph
from cascade.frontend.generator.unparser import Unparser
from cascade.frontend.ast_visitors import ContainsAttributeVisitor, VariableGetter
from cascade.frontend.generator.split_function import SplitFunction
from cascade.frontend.ast_visitors.replace_name import ReplaceName

from cascade.dataflow.dataflow import DataFlow, Edge, InvokeMethod, OpNode, MergeNode

from klara.core import nodes
from klara.core.cfg import RawBasicBlock

class GenerateSplittFunctions:

    def __init__(self, dataflow_graph: DataflowGraph):
        self.dataflow_graph: DataflowGraph = dataflow_graph
        self.dataflow_node_map = dict()
    
    def generate_entity_functions(self):
        self.reveal_block_color()
        self.name_block_nodes()
        self.extract_block_entity_names()
        self.extract_in_out_vars()
        self.replace_entity_var_name_by_state_in_block()
        methods: str = self.compile_methods()
        df: DataFlow = self.create_dataflow()
        return methods, df

    def compile_methods(self):
        compiled_classes = {}
        for node in self.dataflow_graph.get_nodes():
            node: Block
            color = node.color
            if color in [0, -1]:
                entity_type = 'StatelessEntity'
            elif color in self.dataflow_graph.color_type_map:
                entity_type: str = self.get_entity_type_from_color(color)
            else:
                raise Exception(f'There is no entity for color: {color} in color map: {self.dataflow_graph.color_type_map}')
            if not entity_type in compiled_classes:
                compiled_classes[entity_type] = {}
            split_function_name = node.get_name()
            split_function = self.get_split_fuction(node)
            split_function.set_class_name(entity_type)
            compiled_classes[entity_type][split_function_name] = split_function
            # add a dataflow node to dataflow_node_map
            self.dataflow_node_map[node.block_num] = OpNode(entity_type, InvokeMethod(split_function_name))
        return compiled_classes
    
    def create_dataflow(self):
        df = DataFlow('new_dataflow')
        for n, v in self.dataflow_graph.graph.edges():
            op_node_n: OpNode = self.dataflow_node_map[n.block_num]
            op_node_v: OpNode = self.dataflow_node_map[v.block_num]
            edge = Edge(op_node_n, op_node_v)
            df.add_edge(edge)
        return df

    def get_split_fuction(self, node: Block) -> SplitFunction:
        in_vars = node.in_vars
        body = self.get_function_code(node) 
        split_function: SplitFunction = SplitFunction(node.block_num, node.get_name(), body, in_vars) 
        return split_function

    def get_function_code(self, node: Block): 
        body = Unparser.unparse(node.statement_list)
        if not list(self.dataflow_graph.graph.successors(node)):
            return body
        if body == '':
            separator = ''
        else:
            separator = '\n'
        return body + f'{separator}{self.get_successor_keys_for_key_stack(node)})'
    
    def get_successors_for_node(self, node: Block):
        return self.dataflow_graph.graph.successors(node)

    def get_successor_entity_var_names(self, node: Block):
        return [next_node.entity_var_name for next_node in self.get_successors_for_node(node)]
 
    def get_successor_keys_for_key_stack(self, node: Block):
        successor_entity_list = self.get_successor_entity_var_names(node)
        if len(successor_entity_list) == 1:
            entity, = successor_entity_list
            return f'key_stack.append({entity}.key)'
        keys = ', '.join(f'{s}.key' for s in successor_entity_list)
        return f'key_stack.append([{keys}])'
    
    def get_entity_type_from_color(self, color: int):
        color_type_map: dict[int, str] = self.dataflow_graph.color_type_map
        return color_type_map[color]
    
    def name_block_nodes(self):
        counter = count()
        name_prefix = 'compiled_method'
        for node in self.dataflow_graph.get_nodes():
            i: int = next(counter)
            name = f'{name_prefix}_{i}'
            node.set_name(name)
        
    def extract_block_entity_names(self):
        for node in self.dataflow_graph.get_nodes():
            entity_names = set()
            for statement in node.statement_list:
                statement: Statement
                if type(statement.block) == nodes.FunctionDef:
                    continue
                contains_attribute, attribute = ContainsAttributeVisitor.check_return_attribute(statement.block)
                if contains_attribute:
                    entity_names.add(attribute)
            if not entity_names:
                node.set_entity_var_name('stateless_entity')
                continue
            # assert len(entity_names) == 1, "This method assumes each split function only contains one Entity invocation"
            name, *rest  = entity_names
            node.set_entity_var_name(repr(name))
    
    def extract_in_out_vars(self):
        for node in self.dataflow_graph.get_nodes():
            node: Block
            all_vars = set() #all variables right side of assign.
            produced = set() # all variables assigned to
            for statement in node.statement_list:
                statement: Statement
                block: RawBasicBlock = statement.block
                if type(block) == nodes.FunctionDef:
                    # If type is FunctionDef input should be the functionvars
                    vars_ = {str(v) for v in statement.values}
                    all_vars.update(vars_)
                    continue
                variable_getter: VariableGetter = VariableGetter.get_variable(block)
                # add all variables to all_vars
                all_vars.update(repr(v) for v in variable_getter.values)
                # add new vars produced by assign to produced set 
                produced.update(repr(v) for v in variable_getter.targets)
            
            in_vars = all_vars - produced # SSA guarentees produced vars to not be live before this block.
            in_vars = in_vars - {node.entity_var_name}
            node.set_in_vars(in_vars)
            node.set_out_vars(produced)
    
    def reveal_block_color(self):
        for block in self.dataflow_graph.get_nodes():
            block: Block
            block.reveal_color()
    
    def replace_entity_var_name_by_state_in_block(self):
        """ Entities are provided by "state" variable in compiled methods.
            The entity var name should therefore be replaced by state.
            e.g.: item_0.price -> state.price.
        """
        for block in self.dataflow_graph.get_nodes():
            block: Block
            for statement in block.statement_list:
                statement: Statement
                ReplaceName.replace(statement.block, block.entity_var_name, 'state')



    @classmethod
    def generate(cls, dataflow_graph: DataflowGraph):
        c = cls(dataflow_graph)
        return c.generate_entity_functions()

    @staticmethod 
    def generate_split_function_string(dataflow_graph: DataflowGraph):
        res = ''
        split_functions, df = GenerateSplittFunctions.generate(dataflow_graph)
        for split_f in chain.from_iterable(f.values() for f in split_functions.values()):
            split_f: SplitFunction
            res += split_f.to_string() + '\n\n'
        
        return res, df



        



