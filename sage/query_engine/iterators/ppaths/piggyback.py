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
    """A PiggyBackIterator collects data from a PTC iterator to build the control tuples that will be sent to the client.

    It can be used as the starting iterator in a pipeline of iterators.

    Args:
      * child: A PTC iterator.
      * control_tuples: A shared memory where control tuples of all PTC iterators are stored.
      * current_binding: A set of solution mappings. Used to bind the PTC path pattern variables. 
      * mu: A partial solution mappings returned by the PTC iterator.
    """

    def __init__(self, child: PreemptableIterator, control_tuples: ControlTuplesBuffer, current_binding: Optional[Dict[str, str]] = None, mu: Optional[Dict[str, str]] = None):
        super(PiggyBackIterator, self).__init__()
        self._identifier = uuid4()
        self._control_tuples = control_tuples
        self._child = child
        self._current_binding = current_binding
        self._mu = mu

    def next_stage(self, binding: Dict[str, str]):
        """Set the current binding and reset the scan iterator. Used to compute the nested loop joins"""
        self._current_binding = binding
        self._child.next_stage(binding)

    def __repr__(self) -> str:
        return f"<PiggyBackIterator {self._child}>"

    def serialized_name(self):
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "piggyback"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._child.has_next() or self._mu is not None

    def _create_control_tuple(self, node, depth):
        context = dict()
        if self._current_binding is not None:
            context = self._current_binding
        elif self._child._subject.startswith('?'):
            context[self._child._subject] = self._child.get_source()
        return self._control_tuples.create_control_tuple(
            self._child._path_pattern_id,
            context, 
            node, 
            depth + 1, 
            self._child._max_depth, 
            self._child._forward
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
            (partial_mappings, is_final_solution, visited_node, depth) = await self._child.next()
            if partial_mappings is None:
                return None
            self._mu = partial_mappings if is_final_solution else None
            control_tuple = self._create_control_tuple(visited_node, depth)
            self._control_tuples.add(control_tuple)
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