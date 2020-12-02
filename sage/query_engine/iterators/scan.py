# scan.py
# Author: Thomas MINIER, Julien AIMONIER-DAVAT - MIT License 2017-2020
from datetime import datetime
from typing import Dict, Optional

from sage.database.core.dataset import Dataset
from sage.database.db_iterator import DBIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import selection, vars_positions, find_in_mappings, tuple_to_triple, EmptyIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedScanIterator, TriplePattern
from sage.query_engine.protobuf.utils import pyDict_to_protoDict

class ScanIterator(PreemptableIterator):
    """A ScanIterator evaluates a triple pattern over a RDF graph.

    It can be used as the starting iterator in a pipeline of iterators.

    Args:
      * pattern: The triple pattern to evaluate.
      * dataset: The RDF dataset on which the triple pattern is evaluated.
      * current_binding: A set of solution mappings. Used by the nested loop joins to bind the triple pattern variables. 
      * cardinality: The cardinality of the triple pattern given the current binding.
      * progess: The number of triples read on the database. Used to monitor the query.
      * last_read: The last triple read on the database. Used to resume the scan iterator.
      * as_of: The timestamp of the query. Used to read a consistent snapshot of the database (MVCC).
    """

    def __init__(self, pattern: Dict[str, str], dataset: Dataset, current_binding: Optional[Dict[str, str]] = None, cardinality: int = 0, progress: int = 0, last_read: Optional[str] = None, as_of: Optional[datetime] = None):
        super(ScanIterator, self).__init__()
        self._pattern = pattern
        self._dataset = dataset
        self._variables = vars_positions(pattern['subject'], pattern['predicate'], pattern['object'])
        self._current_binding = current_binding
        self._cardinality = cardinality
        self._progress = progress
        self._last_read = last_read
        self._start_timestamp = as_of
        # Create an iterator on the database
        if current_binding is None:
            it, card = dataset.get_graph(pattern['graph']).search(pattern['subject'], pattern['predicate'], pattern['object'], last_read=last_read, as_of=as_of)
            self._source = it
            self._cardinality = card
        else:
            (s, p, o) = (find_in_mappings(pattern['subject'], current_binding), find_in_mappings(pattern['predicate'], current_binding), find_in_mappings(pattern['object'], current_binding))
            it, card = dataset.get_graph(pattern['graph']).search(s, p, o, last_read=last_read, as_of=as_of)
            self._source = it
            self._cardinality = card

    def next_stage(self, binding: Dict[str, str]):
        """Set the current binding and reset the scan iterator. Used to compute the nested loop joins"""
        (s, p, o) = (find_in_mappings(self._pattern['subject'], binding), find_in_mappings(self._pattern['predicate'], binding), find_in_mappings(self._pattern['object'], binding))
        it, card = self._dataset.get_graph(self._pattern['graph']).search(s, p, o, as_of=self._start_timestamp)
        self._current_binding = binding
        self._source = it
        self._cardinality = card
        self._progress = 0
        self._last_read = None

    def __len__(self) -> int:
        return self._cardinality

    def __repr__(self) -> str:
        return f"<ScanIterator ({self._pattern['subject']} {self._pattern['predicate']} {self._pattern['object']}) -> {self._cardinality}>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "scan"

    def last_read(self) -> str:
        return self._source.last_read()

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
            return None
        triple = next(self._source)
        if triple is None:
            return None
        self._progress+=1
        return selection(triple, self._variables)

    def save(self) -> SavedScanIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_scan = SavedScanIterator()
        pattern = TriplePattern()
        pattern.subject = self._pattern['subject']
        pattern.predicate = self._pattern['predicate']
        pattern.object = self._pattern['object']
        pattern.graph = self._pattern['graph']
        saved_scan.pattern.CopyFrom(pattern)
        if self._current_binding is not None:
            pyDict_to_protoDict(self._current_binding, saved_scan.mu)
        saved_scan.cardinality = self._cardinality
        saved_scan.progress = self._progress
        saved_scan.last_read = self._source.last_read()
        if self._start_timestamp is not None:
            saved_scan.timestamp = self._start_timestamp.isoformat()
        return saved_scan