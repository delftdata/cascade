from dataclasses import dataclass

@dataclass
class DataflowGraphBuildContext:
    instance_type_map: dict[str, str] # {"instance_name": "EntityType"} 

    def get_entity_for_var_name(self, name):
        return self.instance_type_map[name]
    