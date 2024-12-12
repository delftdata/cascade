from dataclasses import dataclass, field
from klara.core.cfg import RawBasicBlock


@dataclass
class Block:
    statement_list: list["Statement"]
    block_num: int
    color: int = 0
    method_name: str = None
    entity_var_name: str = None
    in_vars: set = None
    out_vars: set = None
    # entity_type: 

    def set_statement_parents(self):
        """ Sets this block as parents for the statements in statement_list
        """
        for statement in self.statement_list:
            statement.set_parent_block(self)
    
    def reveal_color(self):
        """ Assigns the color of the statements to the color of this block.
            Assumes that all statements have the same color.
        """
        color_set: set[int] = set(s.color for s in self.statement_list)
        assert len(color_set) == 1, "All statments of a block should have the same color"
        color, = color_set
        self.color = color
    
    def set_name(self, name: str):
        self.method_name = name
    
    def get_name(self) -> str:
        return self.method_name
    
    def set_entity_var_name(self, name: str):
        self.entity_var_name = name
    
    def set_in_vars(self, in_vars: set):
        self.in_vars = in_vars
    
    def set_out_vars(self, out_vars: set):
        self.out_vars = out_vars

    def __hash__(self):
        return hash(self.block_num)
    