from dataclasses import dataclass

@dataclass
class DataflowGraphBuildContext:
    scope: str
    entity_list: list[str]
    instance_type_map: dict[str, str] # {"instance_name": "EntityType"} 

    def get_entity_for_var_name(self, name):
        return self.instance_type_map[name]
    
    def is_entity(self, name: str):
        type_: str = self.get_entity_for_var_name(name)
        return type_ in self.entity_list 
