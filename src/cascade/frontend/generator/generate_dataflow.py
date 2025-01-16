from cascade.frontend.generator.split_function import SplitFunction
from cascade.dataflow.dataflow import DataFlow, OpNode, InvokeMethod, Edge


class GenerateDataflow:
    """ Generates dataflow
    """
    
    def __init__(self, split_functions: list[SplitFunction], instance_type_map: dict[str, str]):
        #TODO: add buildcontext that contains class name and target method
        self.split_functions = split_functions
        self.instance_type_map = instance_type_map
    
    def generate_dataflow(self):
        self.extract_remote_method_calls()
        return self.build_dataflow()
    
    def build_dataflow(self):
        """ Every remote function invocation should add the node
        """
        nodes = []
        for split in self.split_functions:
            node = OpNode(split.class_name, InvokeMethod(split.method_name))
            nodes.append([node])

            if split.remote_calls:
                # TODO: instance_name -> correct entity (maybe using buildcontext/ instance type map)
                next_nodes = [OpNode(self.instance_type_map[remote.instance_name], InvokeMethod(remote.attribute), assign_result_to=remote.target) 
                              for remote in split.remote_calls]
                nodes.append(next_nodes)
        return nodes

    def link_nodes(self, nodes):
        self.df.entry = nodes[0][0]
        for i in range(len(nodes)-1):
            # TODO: add merge nodes
            prev_nodes = nodes[i]
            next_nodes = nodes[i+1]
            for n in prev_nodes:
                for v in next_nodes:
                    # TODO: Add variable map (think that should be the aggregation of the targets)
                    self.df.add_edge(Edge(n, v))

    def extract_remote_method_calls(self):
        for split in self.split_functions:
            split.extract_remote_method_calls()
    
    @classmethod
    def generate(cls, split_functions: list[SplitFunction], instance_type_map: dict[str, str]) -> DataFlow:
        c = cls(split_functions, instance_type_map)
        return c.generate_dataflow()