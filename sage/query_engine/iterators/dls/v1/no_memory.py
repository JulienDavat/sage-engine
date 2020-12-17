# transitive_closure.py
# Author: Julien AIMONIER-DAVAT - MIT License 2017-2020
import time
from typing import Dict, List, Optional, Tuple

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import (SavedTransitiveClosureIterator)
from sage.query_engine.protobuf.utils import pyDict_to_protoDict

class TransitiveClosureIterator(PreemptableIterator):
    """A TransitiveClosureIterator evaluates the transitive closure of a relation
    defined by a property path expression.

    It can be used as the starting iterator in a pipeline of iterators.

    Args:
      * subject: The node from which all paths must start. A variable at the subject position means 
        that the transitive closure is evaluated from all the nodes.
      * obj: The node to which all paths must end. A variable at the object position means that all
        the paths are part of the final result.
      * iterators: A list of preemptable iterators used to evaluate the transitive closure. 
      * var_prefix: A common prefix used by all iterators to identify the variables whose values are
        the current path nodes.
      * bindings: The last solution mappings generated by each iterator.
      * current_depth: The depth of the current path.
      * min_depth: The minimum depth for a path to be part of the final result.
      * max_depth: The maximum depth for a path to be part of the final result.
      * complete: False if there is a path of length (max_depth + 1) that match the transitive closure
        expression, True otherwise.
    """

    def __init__(self, subject: str, path: str, obj: str, forward: bool, iterators: List[PreemptableIterator], var_prefix: str, mu: Optional[Dict[str, str]] = None, bindings: List[Dict[str, str]] = None, current_depth: int = 0, min_depth: int = 1, max_depth: int = 10, complete: bool = True, id: Optional[int] = None):
        super(TransitiveClosureIterator, self).__init__()
        print('NoMemory')
        self._id = time.time_ns() if id is None else id
        self._subject = subject
        self._path = path
        self._obj = obj
        self._forward = forward
        self._iterators = iterators
        self._var_prefix = var_prefix
        self._mu = mu
        self._bindings = bindings if bindings is not None else [None] * (max_depth + 1)
        self._current_depth = current_depth
        self._min_depth = min_depth
        self._max_depth = max_depth
        self._complete = complete
        self._reached = dict()

    def __len__(self) -> int:
        """Get an approximation of the result's cardinality of the iterator"""
        if not self._subject.startswith('?') or not self._obj.startswith('?'):
            return 1
        else: 
            return self._iterators[0].__len__() * ( 1 + ((self._max_depth * 10) / 100))

    def __repr__(self) -> str:
        return f"<TransitiveClosureIterator:NoMemory [{self._min_depth}:{self._max_depth}] ({self._iterators})>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "transitive_closure"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._current_depth > 0 or self._iterators[0].has_next()

    def next_stage(self, binding: Dict[str, str]):
        """Set the current binding and reset the scan iterator. Used to compute the nested loop joins"""
        self._bindings = [None] * (self._max_depth + 1)
        self._current_depth = 0
        self._reached = dict()
        self._iterators[0].next_stage(binding)
        self._mu = binding

    def goal_has_been_reached(self):
        source = self.get_source()
        if source not in self._reached:
            return False
        return True

    def goal_reached(self):
        source = self.get_source()
        self._reached[source] = None

    def must_explore(self, node):
        if self.goal_has_been_reached():
            return False
        for depth in range (self._current_depth):
            previous = self.get_node(depth)
            if node == previous:
                return False
        return True

    def get_source(self) -> str:
        """Return the first node of the current path"""
        if self._subject.startswith('?'):
            return self._bindings[0][self._subject]
        else:
            return self._subject

    def get_node(self, depth):
        variable = f'?{self._var_prefix}{depth}'
        return self._bindings[depth][variable]

    def is_solution(self, node: str) -> bool:
        goal = self._obj
        if goal.startswith('?') and self._mu is not None and goal in self._mu:
            goal = self._mu[goal]
        if goal.startswith('?'):
            return True
        elif node == goal:
            self.goal_reached()
            return True
        else:
            return False

    def build_solution(self, node: str) -> Dict[str, str]:
        solution = {}
        if self._subject.startswith('?'):
            solution[self._subject] = self._bindings[0][self._subject]
        if self._obj.startswith('?'):
            solution[self._obj] = node
        if self._mu is not None:
            return {**self._mu, **solution}
        else:
            return solution

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
                if not self.must_explore(node):
                    self._bindings[depth] = None
                    return None, False, 0
                self._iterators[depth + 1].next_stage(current_binding)
                if depth == self._max_depth - 1:
                    self._complete = self._complete and not self._iterators[depth + 1].has_next()
                else:
                    self._current_depth = depth + 1
                solution = self.build_solution(node)
                return solution, self.is_solution(node), depth
            else:
                self._current_depth = depth - 1
        return None, False, 0

    def save(self) -> SavedTransitiveClosureIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_transitive = SavedTransitiveClosureIterator()
        saved_transitive.subject = self._subject
        saved_transitive.path = self._path
        saved_transitive.obj = self._obj
        saved_transitive.forward = self._forward
        saved_iterators = []
        for it in self._iterators:
            saved_it = SavedTransitiveClosureIterator.PreemptableIterator()
            it_field = it.serialized_name() + '_iterator'
            getattr(saved_it, it_field).CopyFrom(it.save())
            saved_iterators.append(saved_it)
        saved_transitive.iterators.extend(saved_iterators)
        saved_transitive.var_prefix = self._var_prefix
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_transitive.mu)
        saved_bindings = []
        for i in range(0, self._current_depth):
            saved_binding = SavedTransitiveClosureIterator.Bindings()
            pyDict_to_protoDict(self._bindings[i], saved_binding.binding)
            saved_bindings.append(saved_binding)
        saved_transitive.bindings.extend(saved_bindings)
        saved_transitive.current_depth = self._current_depth
        saved_transitive.min_depth = self._min_depth
        saved_transitive.max_depth = self._max_depth
        saved_transitive.complete = self._complete
        saved_transitive.id = self._id
        return saved_transitive