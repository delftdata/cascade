from dataclasses import dataclass

@dataclass
class RemoteCall:
    instance_name: str
    attribute: str
    target: str