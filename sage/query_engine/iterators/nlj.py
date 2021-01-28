# nlj.py
# Author: Thomas MINIER, Julien AIMONIER-DAVAT - MIT License 2017-2020
from datetime import datetime
from typing import Dict, Optional, List

from sage.database.core.graph import Graph
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.utils import (EmptyIterator, find_in_mappings,
                                               tuple_to_triple)
from sage.query_engine.primitives import PreemptiveLoop
from sage.query_engine.protobuf.iterators_pb2 import (SavedIndexJoinIterator,
                                                      TriplePattern)
from sage.query_engine.protobuf.utils import pyDict_to_protoDict


class IndexJoinIterator(PreemptableIterator):
    """A IndexJoinIterator implements an Index Loop join in a pipeline of iterators.

    Args:
      * left: left operand of the join
      * right: right operand of the join
      * current_binding: A set of solution mappings used to resume the join processing.
    """

    def __init__(self, left: PreemptableIterator, right: PreemptableIterator, current_binding: Optional[Dict[str, str]] = None):
        super(IndexJoinIterator, self).__init__()
        self._left = left
        self._right = right
        self._current_binding = current_binding

    def __repr__(self) -> str:
        return f"<IndexJoinIterator ({self._left} JOIN {self._right})>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "join"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._left.has_next() or (self._current_binding is not None and self._right.has_next())

    def next_stage(self, binding: Dict[str, str]):
        """Set the current binding and reset the scan iterator. Used to compute the nested loop joins"""
        self._current_binding = None
        self._left.next_stage(binding)

    async def next(self) -> Optional[Dict[str, str]]:
        
        if not self.has_next():
            return None
        with PreemptiveLoop() as loop:
            while self._current_binding is None or (not self._right.has_next()):
                self._current_binding = await self._left.next()
                if self._current_binding is None:
                    return None
                self._right.next_stage(self._current_binding)     
                await loop.tick()
        mu = await self._right.next()
        if mu is not None:
         
            return {**self._current_binding, **mu} # no test of compatibility ?!
        return None

    def save(self) -> SavedIndexJoinIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_join = SavedIndexJoinIterator()
        # export left source
        left_field = self._left.serialized_name() + '_left'
        getattr(saved_join, left_field).CopyFrom(self._left.save())
        # export right source
        right_field = self._right.serialized_name() + '_right'
        getattr(saved_join, right_field).CopyFrom(self._right.save())
        if self._current_binding is not None:
            pyDict_to_protoDict(self._current_binding, saved_join.mu)
        return saved_join
