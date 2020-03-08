# projection.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional, Tuple

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import find_in_mappings, EmptyIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedBindRowSourceIterator
from sage.query_engine.exceptions import UnsupportedSPARQL

import hashlib

def bounded(tp:Tuple[str, str, str]):
    """Return False it it exists a variable in the triple pattern"""
    for var in tp:
        if var.startswith('?'): return False
    return True


class BindRowSourceIterator(PreemptableIterator):
    """A Bind Rowid evaluates a SPARQL BIND (BIND) in a pipeline of iterators.
    ex: BIND(<http://example.org/rowid>(?s,<http://isa>,?o) as ?z)
    Args:
      * source: Previous iterator in the pipeline.
      * tp: triple pattern on which rowid must be determined
      * bindvar: bind variable
    """

    def __init__(self, tp:Tuple[str, str, str], bindvar: str):
        super(BindRowSourceIterator, self).__init__()
        self._tp= tp
        if not(bounded(tp)):
            raise  UnsupportedSPARQL(f"BindRowSource Unsupported SPARQL feature: {tp} not bounded")
        self._bindvar = bindvar
        if bindvar is None:
            raise  UnsupportedSPARQL(f"BindRowSource Unsupported SPARQL feature: bind variable not set")
        self._delivered=False;

    def __repr__(self) -> str:
        return f"<BindRowSourceIdIterator BIND {self._tp} AS {self._bindvar}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "bindrowsourceid"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return not(self._delivered)


    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if not self.has_next():
            raise StopAsyncIteration()

        new_tuple=()
        for var in self._tp:
                new_tuple += (var,)
        tup2str= lambda tup : ''.join(tup)
        # print("hello:"+tup2str(new_tuple))
        mappings=dict()
        mappings[self._bindvar]="http://"+hashlib.md5(tup2str(new_tuple).encode('utf-8')).hexdigest()

        # this iterator deliver only one mapping.
        self._delivered=True;
        return mappings

    def save(self) -> SavedBindRowSourceIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_br = SavedBindRowSourceIterator()
        saved_br.values.extend(self._bindvar)
        saved_br.values.extend(self._tp)
        return saved_br
