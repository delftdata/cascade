from dataclasses import dataclass

@dataclass
class DataflowGraphBuildContext:
    scope: str
    entity_list: list[str]
    type_map: dict[str, str]

    def get_entity_for_var_name(self, name):
        return self.type_map[name]
    
    def is_entity(self, name: str):
        type_: str = self.get_entity_for_var_name(name)
        return type_ in self.entity_list 
