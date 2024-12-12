from dataclasses import dataclass

import networkx as nx

@dataclass
class ClassWrapper:
    name: str
    methods: dict[str, nx.DiGraph]
    attributes: dict[str, str]