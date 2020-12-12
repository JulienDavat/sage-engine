# transitive_closure.py
# Author: Julien AIMONIER-DAVAT - MIT License 2017-2020
import time
from typing import Dict, List, Optional, Tuple

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import (SavedTransitiveClosureIterator, SavedDLS)
from sage.query_engine.protobuf.utils import pyDict_to_protoDict
import sage.query_engine.iterators.loader as loader

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

    def __init__(self, id: int, subject: str, path: PreemptableIterator, obj: str, dataset: Dataset, stack: Optional[List[PreemptableIterator]] = None, bindings: List[Dict[str, str]] = None, source: Optional[str] = None, goal: Optional[str] = None, min_depth: int = 1, max_depth: int = 10, complete: bool = True):
        super(TransitiveClosureIterator, self).__init__()
        print('VisitedNodesMemory')
        self._id = id
        self._subject = subject
        self._path = path
        self._obj = obj
        self._dataset = dataset
        self._bindings = bindings if bindings is not None else [None] * (max_depth + 1)
        self._min_depth = min_depth
        self._max_depth = max_depth
        self._complete = complete
        self._stack = stack
        self._source = source
        self._goal = goal
        # Initialization step
        if self._stack is None:
            if not subject.startswith('?'):
                self._source = subject
                self._path.next_stage({'?source': subject})
            self._stack = [self._path.save()]
            if not obj.startswith('?'):
                self._goal = obj
        self._visited = dict()
        # Initialization of the visited nodes dictionnary
        if bindings is not None and bindings[0] is not None:
            source = self.get_source()
            depth = 0
            while depth < len(bindings) and bindings[depth] is not None:
                node = self.get_node(depth)
                self.mark_as_visited(node)
                depth += 1

    def __len__(self) -> int:
        """Get an approximation of the result's cardinality of the iterator"""
        if not self._subject.startswith('?') or not self._obj.startswith('?'):
            return 1
        return self._path.__len__()

    def __repr__(self) -> str:
        return f"<TransitiveClosureIterator:AdvancedDepthAnnotationMemory [{self._min_depth}:{self._max_depth}] ({self._path})>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "dls"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return len(self._stack) > 0

    def next_stage(self, binding: Dict[str, str]):
        """Set the current binding and reset the scan iterator. Used to compute the nested loop joins"""
        self._bindings = [None] * (self._max_depth + 1)
        if self._subject.startswith('?') and self._subject in binding:
            self._source = binding[self._subject]
        if self._obj.startswith('?') and self._obj in binding:
            self._goal = binding[self._obj]
        if self._source is not None:
            self._path.next_stage({'?source': self._source})
        else:
            self._path.next_stage({})
        self._stack = [self._path.save()]
        self._visited = dict()

    def must_explore(self, node):
        source = self.get_source()
        if source not in self._visited:
            return True
        return node not in self._visited[source]

    def mark_as_visited(self, node):
        source = self.get_source()
        if source not in self._visited:
            self._visited[source] = {}
        self._visited[source][node] = None

    def get_source(self) -> str:
        if self._source is not None:
            return self._source
        else:
            return self._bindings[0]['?source']

    def is_solution(self, node: str) -> bool:
        return (self._goal is None) or (node == self._goal)

    async def next(self) -> Optional[Dict[str, str]]:
        if len(self._stack) > 0:
            iterator = loader.load(self._stack.pop(), self._dataset)
            depth = len(self._stack)
            self._bindings[depth] = None
            if iterator.has_next():
                current_binding = await iterator.next()
                self._bindings[depth] = current_binding
                self._stack.append(iterator.save())
                if current_binding is None:
                    return None
                node = current_binding['?node']
                if not self.must_explore(node):
                    self._bindings[depth] = None
                    return None
                self.mark_as_visited(node)
                if len(self._stack) < self._max_depth:
                    self._path.next_stage({'?source': node})
                    self._stack.append(self._path.save())
                else:
                    self._complete = False
                if self.is_solution(node):
                    solution_mapping = {}
                    if self._subject.startswith('?'):
                        solution_mapping[self._subject] = self.get_source()
                    if self._obj.startswith('?'):
                        solution_mapping[self._obj] = node
                    solution_mapping[f'_depth{self._id}'] = str(depth)
                    return solution_mapping
                return None
        return None

    def save(self) -> SavedDLS:
        """Save and serialize the iterator as a Protobuf message"""
        saved_dls = SavedDLS()
        saved_id = self._id
        saved_dls.subject = self._subject
        saved_path = SavedDLS.PreemptableIterator()
        path_field = self._path.serialized_name() + '_iterator'
        getattr(saved_path, path_field).CopyFrom(self._path.save())
        saved_dls.path.CopyFrom(saved_path)
        saved_dls.obj = self._obj
        saved_dls.source = self._source if self._source is not None else '*'
        saved_dls.goal = self._goal if self._goal is not None else '*'
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
            iterator_field = self._path.serialized_name() + '_iterator'
            getattr(saved_iterator, iterator_field).CopyFrom(self._stack[index])
            saved_stack.append(saved_iterator)
        saved_dls.stack.extend(saved_stack)
        saved_dls.min_depth = self._min_depth
        saved_dls.max_depth = self._max_depth
        saved_dls.complete = self._complete
        return saved_dls