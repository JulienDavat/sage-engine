# reflexive_closure.py
# Author: Julien AIMONIER-DAVAT - MIT License 2017-2020
from typing import Dict, List, Optional, Tuple

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.protobuf.iterators_pb2 import (SavedReflexiveClosureIterator)
from sage.query_engine.protobuf.utils import pyDict_to_protoDict

class ReflexiveClosureIterator(PreemptableIterator):
    """A ReflexiveClosureIterator evaluates the reflexive closure of a relation
    defined by a property path expression.

    It can be used as the starting iterator in a pipeline of iterators.

    Args:
      * source: The node from which all paths must start. A variable at the source position means 
        that the reflexive closure is evaluated from all the nodes.
      * destination: The node to which all paths must end. A variable at the destination position means that all
        the paths are part of the final result.
      * source: A ScanIterator used to retrieve all graph nodes if it's necessary
      * current_binding: The current state of the subject and the object given by the other operators.
      * mu: The next value to return before to read the next mapping of the source.
      * done: True if the reflexive closure has been fully evaluated, False otherwise.
    """

    def __init__(self, source: str, destination: str, child: ScanIterator, mu: Dict[str, str] = None, current_binding: Dict[str, str] = None, done: bool = False):
        super(ReflexiveClosureIterator, self).__init__()
        self._source = source
        self._destination = destination
        self._child = child
        self._current_binding = current_binding
        self._mu = mu
        self._done = done
        self._visited = dict()

    def __len__(self) -> int:
        """Get an approximation of the result's cardinality of the iterator"""
        if not self._source.startswith('?') or not self._destination.startswith('?'):
            return 1
        else:
            return self._child.__len__()

    def __repr__(self) -> str:
        return f"<ReflexiveClosureIterator>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "reflexive_closure"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return not self._done and ( self._child.has_next() or self._mu is not None) 

    def next_stage(self, binding: Dict[str, str]):
        """Set the current binding and reset the scan iterator. Used to compute the nested loop joins"""
        self._current_binding = binding
        self._mu = None
        self._done = False
        self._visited = dict()

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if not self.has_next():
            return None
        # Compute the reflexe closure when either the subject or the object is a constant
        if not self._source.startswith('?') and not self._destination.startswith('?'):
            self._done = True
            return {} if self._source == self._destination else None
        elif not self._source.startswith('?') and self._destination.startswith('?'):
            self._done = True
            if self._current_binding is not None and self._destination in self._current_binding:
                return {} if self._source == self._current_binding[self._destination] else None
            return {self._destination: self._source}
        elif self._source.startswith('?') and not self._destination.startswith('?'):
            self._done = True
            if self._current_binding is not None and self._source in self._current_binding:
                return {} if self._destination == self._current_binding[self._source] else None
            return {self._source: self._destination}
        # Compute the reflexive closure when the subject and the object are unbound variables
        if self._current_binding is None or (self._source not in self._current_binding and self._destination not in self._current_binding):
            if self._mu is None:
                self._mu = await self._child.next()
                node = self._mu['?s']
            else:
                node = self._mu['?o']
                self._mu = None
            if node not in self._visited:
                self._visited[node] = None
                return {self._source: node, self._destination: node}
            return None
        # Compute the reflexive closure when either the subject or the object is bound
        if self._source in self._current_binding and self._destination in self._current_binding:
            self._done = True
            return {} if self._current_binding[self._source] == self._current_binding[self._destination] else None
        elif self._source in self._current_binding and self._destination not in self._current_binding:
            self._done = True
            node = self._current_binding[self._source]
            return {self._source: node, self._destination: node}
        elif self._source not in self._current_binding and self._destination in self._current_binding:
            self._done = True
            node = self._current_binding[self._destination]
            return {self._source: node, self._destination: node}

        return None

    def save(self) -> SavedReflexiveClosureIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_reflexive = SavedReflexiveClosureIterator()
        saved_reflexive.subject = self._source
        saved_reflexive.obj = self._destination
        source_field = self._child.serialized_name() + '_source'
        getattr(saved_reflexive, source_field).CopyFrom(self._child.save())
        saved_reflexive.done = self._done
        if self._current_binding is not None:
            pyDict_to_protoDict(self._current_binding, saved_reflexive.current_binding)
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_reflexive.mu)
        return saved_reflexive