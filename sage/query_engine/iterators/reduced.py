# projection.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional,Set

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedReducedIterator


class ReducedIterator(PreemptableIterator):
    """A ReductedIterator evaluates a SPARQL reduction (REDUCED) in a pipeline of iterators.

    Args:
      * source: Previous iterator in the pipeline.
      * projection: Projection variables
    """
    # mappings=list()

    def __init__(self, source: PreemptableIterator, ):
        super(ReducedIterator, self).__init__()
        self._source = source
        self._memory = dict()
        self._mappings = list()

    def results(self) -> List:
        #for m in self.mappings:
        #    print(f"...{m}...")
        return self._mappings
        # return [dict(s) for s in set(frozenset(d.items()) for d in self._mappings)]

    def __repr__(self) -> str:
        return f"<ReducedIterator FROM {self._source}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "reduc"

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
        mu = await self._source.next()
        # if mu is not None:
        #     self._mappings.append(mu)
        if mu is not None:
            key = str(mu)
            if key not in self._memory:
                self._memory[key] = True
                self._mappings.append(mu)
        return None

    def save(self) -> SavedReducedIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_reduc = SavedReducedIterator()
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_reduc, source_field).CopyFrom(self._source.save())
        return saved_reduc
