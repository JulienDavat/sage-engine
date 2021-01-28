# scan.py
# Author: Thomas MINIER, Julien AIMONIER-DAVAT - MIT License 2017-2020
from datetime import datetime
from uuid import uuid4
from math import inf
from typing import Dict, Optional, List

from sage.database.core.dataset import Dataset
from sage.database.db_iterator import DBIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.utils import selection, vars_positions, find_in_mappings, tuple_to_triple, EmptyIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedPiggyBackIterator
from sage.query_engine.protobuf.utils import pyDict_to_protoDict
from sage.query_engine.exceptions import TooManyResults
from sage.query_engine.iterators.ppaths.control_tuples_memory import ControlTuplesBuffer


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

    def __init__(self, child: PreemptableIterator, control_tuples: ControlTuplesBuffer, current_binding: Optional[Dict[str, str]] = None, mu: Optional[Dict[str, str]] = None):
        super(PiggyBackIterator, self).__init__()
        self._identifiant = uuid4()
        self._control_tuples = control_tuples
        self._child = child
        self._current_binding = current_binding
        self._mu = mu
        self._piggyback = False
        self._last_starter = ""

    def next_stage(self, binding: Dict[str, str]):
        """Set the current binding and reset the scan iterator. Used to compute the nested loop joins"""
        self._current_binding = binding
        self._child.next_stage(binding)
        if self._piggyback:
            self._control_tuples.flush(self._identifiant)
        else:
            self._control_tuples.clear(self._identifiant)
        self._piggyback = False

    def __repr__(self) -> str:
        return f"<PiggyBackIterator {self._child}>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "piggyback"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._child.has_next() or self._mu is not None

    def _create_control_tuple(self, depth):
        context = dict()
        if self._current_binding is not None:
            context = self._current_binding
        elif self._child._subject.startswith('?'):
            context[self._child._subject] = self._child.get_source()
        return self._control_tuples.create_control_tuple(
            context, 
            self._child.get_node(depth), 
            depth + 1, 
            self._child._max_depth, 
            self._child._forward, 
            self._child._subject if self._child._forward else self._child._obj, 
            self._child._path, 
            self._child._obj if self._child._forward else self._child._subject
        )

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
        elif self._child.has_next():
            (solution, is_final_solution, solution_depth) = await self._child.next()
            if solution is None:
                return None

            self._mu = solution if is_final_solution else None

            starter = self._child.get_source()
            if starter != self._last_starter:
                if self._piggyback:
                    print('next source: flushing')
                    self._control_tuples.flush(self._identifiant)
                else:
                    print('next source: clearing')
                    self._control_tuples.clear(self._identifiant)
                self._last_starter = starter
                self._piggyback = False

            control_tuple = self._create_control_tuple(solution_depth)
            self._control_tuples.add(self._identifiant, control_tuple)
            self._piggyback |= (control_tuple['depth'] == control_tuple['max_depth'])
            return None
        else:
            return None


    def save(self) -> SavedPiggyBackIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_piggyback = SavedPiggyBackIterator()
        if self._child is not None:
            source_field = self._child.serialized_name() + '_source'
            getattr(saved_piggyback, source_field).CopyFrom(self._child.save())
        if self._current_binding is not None:
            pyDict_to_protoDict(self._current_binding, saved_piggyback.current_binding)
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_piggyback.mu)
        return saved_piggyback