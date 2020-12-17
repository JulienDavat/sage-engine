# preemptable_iterator.py
# Author: Thomas MINIER - MIT License 2017-2020
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List


class PreemptableIterator(ABC):
    """An abstract class for a preemptable iterator"""

    def __len__(self) -> int:
        """Get an approximation of the result's cardinality of the iterator"""
        return 0

    def __piggyback__(self) -> List[Dict[str, str]]:
        return []

    @abstractmethod
    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        pass

    @abstractmethod
    def next_stage(self, binding: Dict[str, str]):
        """Used to set the scan iterators current binding in order to compute the nested loop joins"""
        pass

    @abstractmethod
    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must 
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        pass

    @abstractmethod
    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        pass

    @abstractmethod
    def save(self) -> Any:
        """Save and serialize the iterator as a Protobuf message"""
        pass
