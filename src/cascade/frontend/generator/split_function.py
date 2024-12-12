from dataclasses import dataclass

@dataclass
class SplitFunction:
    method_name: str
    method_body: str
    in_vars: set[str]