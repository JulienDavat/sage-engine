# transitive_closure.py
# Author: Julien AIMONIER-DAVAT - MIT License 2017-2020
import time
from abc import abstractmethod
from typing import Dict, List, Optional, Tuple

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import (SavedTransitiveClosureIterator, SavedDLS)
from sage.query_engine.protobuf.utils import pyDict_to_protoDict
import sage.query_engine.iterators.loader as loader

class DLSIterator(PreemptableIterator):
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

    def __init__(self, id: int, subject: str, path: str, obj: str, iterator: PreemptableIterator, forward: bool, dataset: Dataset, stack: Optional[List[PreemptableIterator]] = None, mu: Optional[Dict[str, str]] = None, bindings: List[Dict[str, str]] = None, min_depth: int = 1, max_depth: int = 10):
        super(DLSIterator, self).__init__()
        self._id = time.time_ns() if id is None else id
        self._subject = subject
        self._path = path
        self._obj = obj
        self._iterator = iterator
        self._forward = forward
        self._dataset = dataset
        self._stack = stack
        self._mu = mu
        self._bindings = bindings if bindings is not None else [None] * (max_depth + 1)
        self._min_depth = min_depth
        self._max_depth = max_depth
        self._reached = dict()
        # Initialization step
        if self._stack is None:
            if not subject.startswith('?'):
                self._iterator.next_stage({'?source': subject})
            self._stack = [self._iterator.save()]

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return len(self._stack) > 0

    def next_stage(self, binding: Dict[str, str]):
        """Set the current binding and reset the scan iterator. Used to compute the nested loop joins"""
        self._bindings = [None] * (self._max_depth + 1)
        self._mu = binding
        self._reached = dict()
        source = None
        if self._subject.startswith('?') and self._subject in binding:
            source = binding[self._subject]
        if source is not None:
            self._iterator.next_stage({'?source': source})
        else:
            self._iterator.next_stage({})
        self._stack = [self._iterator.save()]
       
    def is_goal_reached(self):
        source = self.get_source()
        if source not in self._reached:
            return False
        return True

    def goal_reached(self):
        source = self.get_source()
        self._reached[source] = None

    def create_cycle(self, node):
        for depth in range (len(self._stack) - 1):
            previous = self.get_node(depth)
            if node == previous:
                return True
        return False

    @abstractmethod
    def must_explore(self, node):
        pass

    def get_source(self) -> str:
        """Return the first node of the current path"""
        if self._subject.startswith('?'):
            return self._bindings[0]['?source']
        else:
            return self._subject

    def get_node(self, depth):
        return self._bindings[depth]['?node']

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
            solution[self._subject] = self._bindings[0]['?source']
        if self._obj.startswith('?'):
            solution[self._obj] = node
        if self._mu is not None:
            return {**self._mu, **solution}
        else:
            return solution

    @abstractmethod
    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must 
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        pass

    def save(self) -> SavedDLS:
        """Save and serialize the iterator as a Protobuf message"""
        saved_dls = SavedDLS()
        saved_dls.id = self._id
        saved_dls.subject = self._subject
        saved_dls.path = self._path
        saved_dls.obj = self._obj
        saved_dls.forward = self._forward
        saved_iterator = SavedDLS.PreemptableIterator()
        path_field = self._iterator.serialized_name() + '_source'
        getattr(saved_iterator, path_field).CopyFrom(self._iterator.save())
        saved_dls.iterator.CopyFrom(saved_iterator)
        saved_bindings = []
        index = 0
        while index < len(self._bindings) and self._bindings[index] is not None:
            saved_binding = SavedDLS.Bindings()
            pyDict_to_protoDict(self._bindings[index], saved_binding.binding)
            saved_bindings.append(saved_binding)
            index += 1
        saved_dls.bindings.extend(saved_bindings)
        saved_stack = []
        for index in range(len(self._stack)):
            saved_iterator = SavedDLS.PreemptableIterator()
            iterator_field = self._iterator.serialized_name() + '_source'
            getattr(saved_iterator, iterator_field).CopyFrom(self._stack[index])
            saved_stack.append(saved_iterator)
        saved_dls.stack.extend(saved_stack)
        saved_dls.min_depth = self._min_depth
        saved_dls.max_depth = self._max_depth
        return saved_dls