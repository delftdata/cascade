import networkx as nx

from cascade.dataflow.dataflow import DataFlow, OpNode, InvokeMethod, Edge
from cascade.descriptors.split_descriptor import SplitDescriptor
from cascade.frontend.dataflow_analysis.split_control_flow import BranchType


class DataflowLinker:


    def __init__(self, class_name: str, control_flow_node_map: dict[str, list[OpNode]], control_flow_split_graph: nx.DiGraph):
        self.class_name: str = class_name
        self.control_flow_node_map: dict[str, list[list[OpNode]]] = control_flow_node_map
        self.control_flow_split_graph: nx.DiGraph = control_flow_split_graph
        self.df = DataFlow(class_name)

    def link_methods(self):
        self.set_entry_node()
        # link inside nodes:
        for split_function_descriptor in self.control_flow_split_graph:
            nodes: list[list[OpNode]] = self.control_flow_node_map[split_function_descriptor.method_name]
            self.link_nodes(nodes)
            
        # link edges
        for n, v, data in self.control_flow_split_graph.edges(data=True, default=BranchType.base):
            edge_type: BranchType = data['edge_type']
            if edge_type == BranchType.if_branch or edge_type == BranchType.else_branch:
                pass
            from_node_list: list[list[OpNode]] = self.control_flow_node_map[n.method_name]
            to_node_list: list[list[OpNode]] = self.control_flow_node_map[v.method_name]
            if from_node_list and to_node_list:
                self.link_outsides(from_node_list[-1], to_node_list[0])
            else:
                print('Is this possible?')

    def set_entry_node(self):
        entry_node_descriptor: SplitDescriptor = next(iter(self.control_flow_split_graph.nodes))
        entry_node_list = self.control_flow_node_map[entry_node_descriptor.method_name]
        entry_op_node, *_ = entry_node_list
        self.df.entry = entry_op_node   

    def link_outsides(self, from_node_list, to_node_list):
        for n in from_node_list:
            for v in to_node_list:
                self.df.add_edge(Edge(n, v))

    def link_nodes(self, nodes):
        if len(nodes)<2:
            return
        for i in range(len(nodes)-1):
            # TODO: add merge nodes
            prev_nodes = nodes[i]
            next_nodes = nodes[i+1]
            for n in prev_nodes:
                for v in next_nodes:
                    # TODO: Add variable map (think that should be the aggregation of the targets)
                    self.df.add_edge(Edge(n, v))
    
    @classmethod
    def link(cls, *args, **kwargs):
        c = cls(*args, **kwargs)
        c.link_methods()
        return c.df
