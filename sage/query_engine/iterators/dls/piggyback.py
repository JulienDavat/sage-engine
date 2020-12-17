# scan.py
# Author: Thomas MINIER, Julien AIMONIER-DAVAT - MIT License 2017-2020
from datetime import datetime
from typing import Dict, Optional, List

from sage.database.core.dataset import Dataset
from sage.database.db_iterator import DBIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import selection, vars_positions, find_in_mappings, tuple_to_triple, EmptyIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedPiggyBackIterator
from sage.query_engine.protobuf.utils import pyDict_to_protoDict
from sage.query_engine.exceptions import TooManyResults

class PiggyBackIterator(PreemptableIterator):
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

    def __init__(self, source: PreemptableIterator, current_binding: Optional[Dict[str, str]] = None, mu: Optional[Dict[str, str]] = None):
        super(PiggyBackIterator, self).__init__()
        self._source = source
        self._current_binding = current_binding
        self._mu = mu
        self._piggyback = False
        self._buffer = []
        self._temp = []
        self._starter = ""

    def next_stage(self, binding: Dict[str, str]):
        """Set the current binding and reset the scan iterator. Used to compute the nested loop joins"""
        self._current_binding = binding
        self._source.next_stage(binding)
        self._piggyback = False
        self._temp = []

    def __len__(self) -> int:
        return self._source.__len__()

    def __repr__(self) -> str:
        return f"<PiggyBackIterator {self._source}>"

    def __piggyback__(self) -> List[Dict[str, str]]:
        print('piggyback: ' + str(len(self._buffer)))
        return self._buffer

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "piggyback"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._source.has_next() or self._mu is not None

    def build_information(self, solution, solution_depth):
        information = dict()
        information['context'] = dict()
        if self._current_binding is not None:
            information['context'] = dict(**self._current_binding).copy()
        elif self._source._subject.startswith('?'):
            information['context'][self._source._subject] = self._source.get_source()
        information['node'] = self._source.get_node(solution_depth)
        information['forward'] = self._source._forward
        information['max_depth'] = self._source._max_depth
        information['depth'] = solution_depth + 1
        information['path'] = dict()
        if self._source._forward:
            information['path']['subject'] = self._source._subject
            information['path']['predicate'] = self._source._path
            information['path']['object'] = self._source._obj
        else:
            information['path']['object'] = self._source._subject
            information['path']['predicate'] = self._source._path
            information['path']['subject'] = self._source._obj
        return information

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if self._mu is not None:
            solution = self._mu
            self._mu = None
            return solution
        elif self._source.has_next():
            (solution, is_final_solution, solution_depth) = await self._source.next()
            if solution is None:
                return None

            starter = self._source.get_source()
            if starter != self._starter:
                self._starter = starter
                self._piggyback = False
                self._temp = []

            information = self.build_information(solution, solution_depth)
            if self._piggyback:
                self._buffer.append(information)
            elif information['depth'] == information['max_depth']:
                self._piggyback = True
                self._buffer.extend(self._temp)
                self._buffer.append(information)
                self._temp = []
            else:
                self._temp.append(information)

            if len(self._buffer) > 10000:
                if is_final_solution:
                    self._mu = solution
                raise TooManyResults()
            elif is_final_solution:
                return solution
            else:
                return None
        else:
            return None

    def save(self) -> SavedPiggyBackIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_piggyback = SavedPiggyBackIterator()
        if self._source is not None:
            source_field = self._source.serialized_name() + '_source'
            getattr(saved_piggyback, source_field).CopyFrom(self._source.save())
        if self._current_binding is not None:
            pyDict_to_protoDict(self._current_binding, saved_piggyback.current_binding)
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_piggyback.mu)
        return saved_piggyback