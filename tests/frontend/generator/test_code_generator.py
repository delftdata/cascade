from tests.context import cascade

# from tests.programs.target.checkout_item import User
import tests.programs.target.checkout_item

from cascade.wrappers import ClassWrapper
from cascade.descriptors import ClassDescriptor, MethodDescriptor
from cascade.frontend.generator.generate_split_functions import GenerateSplittFunctions
from cascade.frontend.generator.generate_dataflow import GenerateDataflow
from cascade.dataflow.dataflow import DataFlow 
from cascade.frontend.intermediate_representation import StatementDataflowGraph
from cascade.frontend.generator.build_compiled_method_string import BuildCompiledMethodsString

def test_if_code_generator_contains_registered_classes():
    classes: list[ClassWrapper] = cascade.core.registered_classes
    assert classes

def test_code_generator():
    cascade.core.init()
    classes: list[ClassWrapper] = cascade.core.registered_classes
    class_name: str = 'User'
    user_class_wrapper: ClassWrapper = next(c for c in classes if c.class_desc.class_name == class_name)
    assert user_class_wrapper
    
    user_class_desc: ClassDescriptor = user_class_wrapper.class_desc
    method_name: str = 'buy_item'
    buy_item_method_desc: MethodDescriptor = user_class_desc.get_method_by_name(method_name)
    dataflow_graph: StatementDataflowGraph = buy_item_method_desc.dataflow
    
    entities: list[str] = cascade.core.get_entity_names()
    split_functions = GenerateSplittFunctions.generate(dataflow_graph, class_name, entities)
    df: DataFlow = GenerateDataflow.generate(split_functions, dataflow_graph.instance_type_map)
    print('dataflow nodes:')
    for n in df.nodes.values():
        print(n)

    print(df.adjacency_list)

    compiled_methods: str = BuildCompiledMethodsString.build(split_functions)
    print(compiled_methods)
    