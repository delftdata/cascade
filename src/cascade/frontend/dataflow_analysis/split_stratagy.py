import abc
import ast
from typing import Tuple

from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, SplitBlock
from cascade.frontend.ast_visitors import ContainsAttributeVisitor

class SplitStratagy:

    def __init__(self, entities: list[str], instane_type_map: dict[str, str]):
        self.entities: list[str] = entities
        self.instance_type_map: dict[str, str] = instane_type_map # e.g.: {"item": "Item"}
        
    @abc.abstractmethod
    def split(self, statements: list[ast.stmt])-> Tuple[SplitBlock, list[ast.stmt]]:
        pass

    def contains_remote_entity_invocation(self, statments: list[ast.stmt]) -> bool:
        """Returns whether statements contains a remote invocation"""
        return any(self.statement_contains_remote_entity_invocation(s) for s in statments)
    
    def statement_contains_remote_entity_invocation(self, s: ast.stmt):
        """Returns True of one of the nodes values contains an remote function invocatinon.
        """
        contains_attribute, attribute = ContainsAttributeVisitor.check_and_return_attribute(s)
        if contains_attribute and attribute.value.id in self.instance_type_map:
            attribute_type = self.instance_type_map[attribute.value.id]
            return attribute_type in self.entities
        return False


class LinearSplitStratagy(SplitStratagy):

    def split(self, statements: list[ast.stmt]) -> Tuple[SplitBlock, list[ast.stmt]]:
        split = []
        while statements:
            statement: ast.stmt = statements.pop(0)
            if self.statement_contains_remote_entity_invocation(statement):
                # Return a split block with the statements in split. The current block containing
                # the  remote function invocation will be turned into a remote function invocation.
                return SplitBlock(split, [statement]), statements
            else:
                split.append(statement)

        assert False, "This method asserts at least one remote entity invocation in the statements."