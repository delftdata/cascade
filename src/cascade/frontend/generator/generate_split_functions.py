from itertools import count

import networkx as nx

from cascade.frontend.intermediate_representation import Block, Statement, DataflowGraph
from cascade.frontend.generator.unparser import Unparser
from cascade.frontend.ast_visitors import ContainsAttributeVisitor, VariableGetter
from cascade.frontend.generator.split_function import SplitFunction

from klara.core import nodes
from klara.core.cfg import RawBasicBlock

class GenerateSplittFunctions:

    def __init__(self, dataflow_graph: DataflowGraph):
        self.dataflow_graph: DataflowGraph = dataflow_graph
    
    def generate_entity_functions(self):
        self.name_block_nodes()
        self.extract_block_entity_names()
        self.extract_in_out_vars()
        return self.compile_methods()
    

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
            compiled_classes[entity_type][split_function_name] = split_function
        return compiled_classes
    
    def get_split_fuction(self, node) -> SplitFunction:
        in_vars = node.in_vars
        body = self.get_function_code(node) 
        split_function: SplitFunction = SplitFunction(node.get_name(), body, in_vars) 
        return split_function

    def get_function_code(self, node: Block): 
        body = Unparser.unparse(node.statement_list)
        next_functions_to_call = self.get_next_functions_to_call(node)
        if not next_functions_to_call:
            return body
        if body == '':
            separator = ''
        else:
            separator = '\n'
        return body + f'{separator}call_remote_async({next_functions_to_call})'
    
    def get_next_functions_to_call(self, node):
        cfg: nx.DiGraph = self.dataflow_graph.graph
        successors = cfg.successors(node)
        functions_to_call = []
        for next_node in successors:
            function_name: str = next_node.get_name()
            attribute: str = next_node.entity_var_name
            name:str = f'{attribute}.{function_name}'
            functions_to_call.append(name)
        return functions_to_call
 
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
                contains_attribute, attribute_name = ContainsAttributeVisitor.check(statement.block)
                if contains_attribute:
                    entity_names.add(attribute_name)
            if not entity_names:
                node.set_entity_var_name('stateless_entity')
                continue
            assert len(entity_names) == 1, "This method assumes each split function only contains one Entity invocation"
            name, = entity_names
            node.set_entity_var_name(name)
    
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
            node.set_in_vars(in_vars)
            node.set_out_vars(produced)

    @classmethod
    def generate(cls, dataflow_graph: DataflowGraph):
        c = cls(dataflow_graph)
        return c.generate_entity_functions()



