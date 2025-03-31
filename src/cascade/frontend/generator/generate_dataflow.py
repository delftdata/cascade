from cascade.dataflow.operator import Block
from cascade.frontend.generator.split_function import SplitFunction
from cascade.dataflow.dataflow import CallEntity, CallLocal, DataFlow, DataflowRef, OpNode, InvokeMethod, Edge


class GenerateDataflow:
    """ Generates dataflow
    """
    
    def __init__(self, split_functions: list[SplitFunction], instance_type_map: dict[str, str], method_name, op_name, args):
        #TODO: add buildcontext that contains class name and target method
        self.split_functions = split_functions
        self.df = DataFlow(method_name, op_name, args)
        self.blocks: list[Block] = []
        self.instance_type_map = instance_type_map
    
    def generate_dataflow(self):
        self.extract_remote_method_calls()
        self.build_dataflow()
    
    def build_dataflow(self):
        """ Every remote function invocation should add the node
        """
        nodes = []
        for split in self.split_functions:
            node = CallLocal(InvokeMethod(split.method_name))
            self.df.add_node(node)
            nodes.append([node])

            if split.remote_calls:
                # TODO: instance_name -> correct entity (maybe using buildcontext/ instance type map)
                next_nodes = []
                for remote in split.remote_calls:
                    df = DataflowRef(self.instance_type_map[remote.instance_name], remote.attribute)
                    args = df.get_dataflow.args
                    # TODO: proper variable renaming 
                    vars = {arg: arg for arg in args}
                    call = CallEntity(df, vars, assign_result_to=remote.target) 
                    next_nodes.append(call)
                nodes.append(next_nodes)

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
    def generate(cls, split_functions: list[SplitFunction], instance_type_map: dict[str, str], method_name, op_name, args) -> tuple[DataFlow, list[Block]]:
        c = cls(split_functions, instance_type_map, method_name, op_name, args)
        c.generate_dataflow()
        return c.df, c.blocks