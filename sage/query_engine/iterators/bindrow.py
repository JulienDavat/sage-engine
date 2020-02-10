# projection.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional, Tuple

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import find_in_mappings
from sage.query_engine.protobuf.iterators_pb2 import SavedBindRowIterator

import hashlib

class BindRowIterator(PreemptableIterator):
    """A ProjectionIterator evaluates a SPARQL projection (SELECT) in a pipeline of iterators.

    Args:
      * source: Previous iterator in the pipeline.
      * projection: Projection variables
    """

    def __init__(self, source: PreemptableIterator, tp:Tuple[str, str, str], bindvar: str):
        super(BindRowIterator, self).__init__()
        self._source = source
        self._tp= tp
        self._bindvar = bindvar

    def __repr__(self) -> str:
        return f"<BindRowIdIterator BIND {self._tp} AS {self._bindvar} FROM {self._source}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "bindrowid"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._source.has_next()

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if not self.has_next():
            raise StopAsyncIteration()
        mappings = await self._source.next()
        if mappings is None:
            return None
        elif self._bindvar is None:
            return mappings
        elif self._tp is None:
            return mappings

        new_tuple=()
        for var in self._tp:
            if var.startswith('?'):
                new_tuple += (find_in_mappings(var,mappings),)
            else:
                new_tuple += (var,)
        tup2str= lambda tup : ''.join(tup)
        # print("hello:"+tup2str(new_tuple))
        mappings[self._bindvar]="http://"+hashlib.md5(tup2str(new_tuple).encode('utf-8')).hexdigest()
        return mappings

    def save(self) -> SavedBindRowIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_br = SavedBindRowIterator()
        saved_br.values.extend(self._bindvar)
        saved_br.values.extend(self._tp)
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_br, source_field).CopyFrom(self._source.save())
        return saved_br
