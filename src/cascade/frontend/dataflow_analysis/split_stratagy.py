import abc
from typing import Tuple

from klara.core import nodes

from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock, SplitBlock
from cascade.frontend.ast_visitors import ContainsAttributeVisitor

class SplitStratagy:

    def __init__(self, entities: list[str]):
        self.entities: list[str] = entities
        
    @abc.abstractmethod
    def split(self, statements: list[nodes.Statement])-> Tuple[SplitBlock, list[nodes.Statement]]:
        pass

    def contains_remote_entity_invocation(self, statments: list[nodes.Statement]) -> bool:
        """Returns whether statements contains a remote invocation"""
        return any(self.statement_contains_remote_entity_invocation(s) for s in statments)
    
    def statement_contains_remote_entity_invocation(self, s: nodes.Statement):
        """Returns True of one of the nodes values contains an remote function invocatinon."""
        contains_attribute, attribute = ContainsAttributeVisitor.check_and_return_attribute(s)
        # TODO this method should use variable type map of method...
        # attributes are in type e.g. item_1. 
        # variable type map should consist all variables and types.
        # And dan see if the type of the attribute is a Remote entity...
        if contains_attribute:
            return attribute in self.entities
        return False


class LinearSplitStratagy(SplitStratagy):

    def split(self, statements: list[nodes.Statement]) -> Tuple[SplitBlock, list[nodes.Statement]]:
        split = []
        while statements:
            statement: nodes.Statement = statements.pop(0)
            if self.statement_contains_remote_entity_invocation(statement):
                # Return a split block with the statements in split. The current block containing
                # the  remote function invocation will be turned into a remote function invocation.
                return SplitBlock(split, [statement]), statements
            else:
                split.append(statement)

        assert False, "This method asserts at least one remote entity invocation in the statements."