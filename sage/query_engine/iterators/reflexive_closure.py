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
      * subject: The path pattern subject.
      * obj: The path pattern object.
    """

    def __init__(self, subject: str, obj: str, source: ScanIterator, mu: Dict[str, str] = None, current_binding: Dict[str, str] = None, done: bool = False):
        super(ReflexiveClosureIterator, self).__init__()
        self._subject = subject
        self._obj = obj
        self._source = source
        self._current_binding = current_binding
        self._mu = mu
        self._done = done
        self._visited = dict()

    def __len__(self) -> int:
        return 0

    def __repr__(self) -> str:
        return f"<ScanIterator ({self._triple['subject']} {self._triple['predicate']} {self._triple['object']})>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "reflexive_closure"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return not self._done and ( self._source.has_next() or self._mu is not None) 

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
        if not self._subject.startswith('?') and not self._obj.startswith('?'):
            self._done = True
            return {} if self._subject == self._obj else None
        elif not self._subject.startswith('?') and self._obj.startswith('?'):
            self._done = True
            if self._current_binding is not None and self._obj in self._current_binding:
                return {} if self._subject == self._current_binding[self._obj] else None
            return {self._obj: self._subject}
        elif self._subject.startswith('?') and not self._obj.startswith('?'):
            self._done = True
            if self._current_binding is not None and self._subject in self._current_binding:
                return {} if self._obj == self._current_binding[self._subject] else None
            return {self._subject: self._obj}
        # Compute the reflexive closure when the subject and the object are unbound variables
        if self._current_binding is None or (self._subject not in self._current_binding and self._obj not in self._current_binding):
            if self._mu is None:
                self._mu = await self._source.next()
                node = self._mu['?s']
            else:
                node = self._mu['?o']
                self._mu = None
            if node not in self._visited:
                self._visited[node] = None
                return {self._subject: node, self._obj: node}
            return None
        # Compute the reflexive closure when either the subject or the object is bound
        if self._subject in self._current_binding and self._obj in self._current_binding:
            self._done = True
            return {} if self._current_binding[self._subject] == self._current_binding[self._obj] else None
        elif self._subject in self._current_binding and self._obj not in self._current_binding:
            self._done = True
            node = self._current_binding[self._subject]
            return {self._subject: node, self._obj: node}
        elif self._subject not in self._current_binding and self._obj in self._current_binding:
            self._done = True
            node = self._current_binding[self._obj]
            return {self._subject: node, self._obj: node}

        return None

    def save(self) -> SavedReflexiveClosureIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_reflexive = SavedReflexiveClosureIterator()
        saved_reflexive.subject = self._subject
        saved_reflexive.obj = self._obj
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_reflexive, source_field).CopyFrom(self._source.save())
        saved_reflexive.done = self._done
        if self._current_binding is not None:
            pyDict_to_protoDict(self._current_binding, saved_reflexive.current_binding)
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_reflexive.mu)
        return saved_reflexive