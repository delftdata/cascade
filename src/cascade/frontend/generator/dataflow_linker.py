import networkx as nx
from cascade.dataflow.dataflow import DataFlow, OpNode, InvokeMethod, Edge
from cascade.descriptors.split_descriptor import SplitDescriptor


class DataflowLinker:


    @staticmethod
    def link_methods(class_name: str, control_flow_node_map: dict[str, list[OpNode]], control_flow_split_graph: nx.DiGraph):
        df: DataFlow = DataFlow(class_name)
        entry_node_descriptor: SplitDescriptor = next(iter(control_flow_split_graph.nodes))
        entry_node_list = control_flow_node_map[entry_node_descriptor.method_name]
        assert(len(entry_node_list)) == 1, 'There should be one entry node.'
        entry_op_node,  = entry_node_list
        df.entry = entry_op_node
        visit_stack = [entry_node_descriptor]
        
        while visit_stack:
            node: SplitDescriptor = visit_stack.pop(0)
            op_node_list: list[OpNode] = control_flow_node_map[node.method_name]
            for _, out_node in control_flow_split_graph.edges(node):
                # make a method that gets the first and the last node of every control_flow_node_map list and than go through each
                # edge and link the first and the last together
                # also link the insides

                out_node_op_list: list[OpNode] = control_flow_node_map[out_node.method_name]
                DataflowLinker.link(out_node_op_list, op_node_list)





    def link(self, nodes_list, previous_nodes):
        for nodes in nodes_list:
            for n in previous_nodes:
                for v in nodes:
                    # TODO: Add variable map (think that should be the aggregation of the targets)
                    self.df.add_edge(Edge(n, v))
            previous_nodes = nodes
        return previous_nodes