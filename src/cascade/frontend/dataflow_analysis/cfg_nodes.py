from dataclasses import dataclass

from klara.core.nodes import Statement

class Block:
    statements: list[Statement]
    next_block: "Block" = None

class IFBlock:

    if_condition: Statement
    body: list[Statement]
    or_else: list[Statement]
    next_block: "Block" = None

class ForLoop:
    loop_condition: Statement
    body: list[Statement] 
    next_block: "Block" = None
