# transitive_closure.py
# Author: Julien AIMONIER-DAVAT - MIT License 2017-2020
import time
from typing import Dict, List, Optional, Tuple

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.ppaths.v1.dls import DLSIterator
from sage.query_engine.protobuf.iterators_pb2 import (SavedTransitiveClosureIterator)
from sage.query_engine.protobuf.utils import pyDict_to_protoDict

class TransitiveClosureIterator(DLSIterator):
    """A TransitiveClosureIterator evaluates the transitive closure of a relation
    defined by a property path expression.

    It can be used as the starting iterator in a pipeline of iterators.

    Args:
      * source: The node from which all paths must start. A variable at the source position means 
        that the transitive closure is evaluated from all the nodes.
      * destination: The node to which all paths must end. A variable at the destination position means that all
        the paths are part of the final result.
      * iterators: A list of preemptable iterators used to evaluate the transitive closure. 
      * bindings: The last solution mappings generated by each iterator.
      * current_depth: The depth of the current path.
      * min_depth: The minimum depth for a path to be part of the final result.
      * max_depth: The maximum depth for a path to be part of the final result.
    """

    def __init__(self, source: str, path: str, destination: str, forward: bool, iterators: List[PreemptableIterator], mu: Optional[Dict[str, str]] = None, bindings: List[Dict[str, str]] = None, current_depth: int = 0, min_depth: int = 1, max_depth: int = 10, id: Optional[int] = None):
        super(TransitiveClosureIterator, self).__init__(source, path, destination, forward, iterators, mu, bindings, current_depth, min_depth, max_depth, id)
        print('NoMemory')

    def __repr__(self) -> str:
        return f"<TransitiveClosureIterator:NoMemory [{self._min_depth}:{self._max_depth}] ({self._iterators})>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "transitive_closure"

    def must_explore(self, node):
        return self.create_cycle(node)

    async def next(self) -> Optional[Dict[str, str]]:
        if self.has_next():
            depth = self._current_depth
            self._bindings[depth] = None
            if self._iterators[depth].has_next():
                current_binding = await self._iterators[depth].next()
                if current_binding is None:
                    return None, False, 0
                self._bindings[depth] = current_binding
                node = self.get_node(depth)
                if not self.must_explore(node) or self.is_goal_reached():
                    self._bindings[depth] = None
                    return None, False, 0
                self._iterators[depth + 1].next_stage(current_binding)
                self._current_depth = depth + 1 if depth < (self._max_depth + 1) else depth
                solution = self.build_solution(node)
                return solution, self.is_solution(node), depth
            else:
                self._current_depth = depth - 1
        return None, False, 0