import copy 

import networkx as nx
from klara.core import nodes
from enum import Enum, auto


from cascade.descriptors.split_descriptor import SplitDescriptor
from cascade.frontend.ast_visitors import VariableGetter

class BranchType(Enum):
    if_branch = auto()
    else_branch = auto()
    base = auto()


class SplitControlFlow:
    
    def __init__(self):
        self.G = nx.DiGraph()
    
    def split_function_body(self, body: list[str], method_name: str, live_variables: list, previous_nodes=[], branch_type: BranchType=BranchType.base):
        live_variables = copy.copy(live_variables)
        new_live_variables = []
        i = 0
        split = []
        G: nx.DiGraph = self.G
        for node in body:
            match type(node):
                case nodes.If: 
                    if split:
                        if i == 0:
                            new_split = method_name
                        else:
                            new_split = method_name + str(i)
                        new_split_desciptor: SplitDescriptor = SplitDescriptor(new_split, split, live_variables)
                        G.add_node(new_split_desciptor)
                        previous_nodes = self.add_node_to_graph(previous_nodes, G, new_split_desciptor, branch_type)
                        branch_type = BranchType.base
                    
                    if_cond_node: nodes.Return = nodes.Return()
                    if_cond_node.postinit(value=node.test)
                    if_cond_split: SplitDescriptor = SplitDescriptor(method_name + '_if_cond', [if_cond_node], live_variables, is_if_condition=True)
                    
                    previous_nodes = self.add_node_to_graph(previous_nodes, G, if_cond_split, branch_type)
                    branch_type = BranchType.base

                    if_body_prev_nodes = self.split_function_body(node.body, 
                                                                  method_name + '_body', 
                                                                  new_live_variables + live_variables, 
                                                                  previous_nodes=previous_nodes, 
                                                                  branch_type=BranchType.if_branch)
                    else_body_prev_nodes = self.split_function_body(node.orelse, 
                                                                    method_name + '_orelse', 
                                                                    new_live_variables + live_variables, 
                                                                    previous_nodes=previous_nodes, 
                                                                    branch_type=BranchType.else_branch)
                    previous_nodes = if_body_prev_nodes + else_body_prev_nodes
                    
                    split = []
                    i += 1
                case _:
                    split.append(node)
            variable_getter = VariableGetter.get_variable(node)
            new_live_variables.extend(variable_getter.targets)
        if split:
            if i == 0:
                new_node = method_name
            else:
                new_node = method_name+str(i)

            split_desciptor: SplitDescriptor = SplitDescriptor(new_node, split, live_variables + new_live_variables)

            previous_nodes = self.add_node_to_graph(previous_nodes, G, split_desciptor, branch_type)
        
        return previous_nodes

    def add_node_to_graph(self, previous_nodes, G, new_node, branch_type: BranchType):
        G.add_node(new_node)
        for p in previous_nodes:
            G.add_edge(p, new_node, edge_type=branch_type)
        return [new_node]


    @classmethod
    def split(cls, function_def: nodes.FunctionDef, method_name: str) -> list[SplitDescriptor]:
        c = cls()
        c.split_function_body(function_def.body, method_name, copy.copy(function_def.args.args))
        return c.G

