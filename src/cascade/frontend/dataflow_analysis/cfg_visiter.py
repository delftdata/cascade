from cascade.frontend.dataflow_analysis.cfg_nodes import BaseBlock
from cascade.frontend.dataflow_analysis.control_flow_graph import ControlFlowGraph


class CFGVisitor:

    def breadth_first_walk(self, cfg: ControlFlowGraph):
         queue = [cfg.entry]
         visited = []
         while queue:
            q: BaseBlock = queue.pop(0)
            if q in visited:
                continue

            self._visit(q)

            # add q to visited nodes.
            visited.append(q)

            # Add children of q to queue.
            for b in q.get_next_blocks():
                 if b != None and b not in visited:
                    queue.append(b)

    def _visit(self, block: BaseBlock):
        """ Visit all blocks fron the cfg with  breath first walk
        """
        method = "visit_" + block.__class__.__name__.lower()
        visitor = getattr(self, method, self.generic_visit)
        visitor(block)
    
    def generic_visit(self, block):
        pass
