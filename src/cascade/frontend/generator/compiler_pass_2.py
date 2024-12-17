
from cascade.frontend.intermediate_representation import Block, Statement, StatementDataflowGraph
from cascade.dataflow.dataflow import OpNode
from cascade.frontend.generator.split_function import SplitFunction
from cascade.frontend.generator.generate_split_functions import GenerateSplittFunctions
from cascade.frontend.ast_visitors.contains_attribute_visitor import ContainsAttributeVisitor
from cascade.frontend.ast_visitors.replace_name import ReplaceName
from cascade.frontend.generator.unparser import Unparser

from klara.core import nodes

from klara.core.cfg import RawBasicBlock

class CompilerPass2:

    def __init__(self, splits: list[list[Statement]]):
        self.splits: list[list[Statement]] = splits
    
    def make_splitfunctions(self) -> list[SplitFunction]:
        """ - extract SplitFunctions
        """
        bodies = [] # (OpNode, SplitFunction (list[str])
        for split in self.splits:
            body = []
            for s in split:
                for v in s.values:
                    if not (repr(v) in [ 'self_0','self']):
                        body.append(f'{v} = variable_map[\'{v}\']')

                if s.remote_call:
                    assert s.attribute_name
                    instance_name = s.attribute_name
                    res = f'key_stack.append(variable_map[\'{instance_name}_key\'])'
                    body.append(res)
                else:
                    block: RawBasicBlock = s.block
                    if type(block) == nodes.FunctionDef:
                        continue
                    ReplaceName.replace(block, 'self', 'state')
                    
                    if type(block) == nodes.Return:
                        body.append('key_stack.pop()') 
                    body.append(Unparser.unparse_block(block))

            bodies.append(body)
        return bodies


    # def make_dataflow(self) -> DataFlow:
        
    #     first, *rest = self.splits
    #     entry: Statement = first.pop(0)
    #     in_vars = entry.targets
    #     pass
 