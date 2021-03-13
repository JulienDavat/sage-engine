# utils.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional, Tuple
from rdflib import BNode, Literal, URIRef, Variable
import hashlib
import re


class EmptyIterator(object):
    """An Iterator that yields nothing"""

    def __init__(self):
        super(EmptyIterator, self).__init__()

    def __len__(self) -> int:
        return 0

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return False

    def next_stage(self, bindings) -> None:
        """Used to set the scan iterators current binding in order to compute the nested loop joins"""
        return

    async def next(self) -> None:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        raise StopAsyncIteration()



class ArrayIterator(object):
    """An iterator that sequentially yields all items from a list.

    Argument: List of solution mappings.
    """

    def __init__(self, array: List[Dict[str, str]]):
        super(ArrayIterator, self).__init__()
        self._array = array

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return len(self._array) > 0

    def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if not self.has_next():
            raise StopAsyncIteration()
        mu = self._array.pop(0)
        return mu


def selection(triple: Tuple[str, str, str], variables: List[str]) -> Dict[str, str]:
    """Apply a selection on a RDF triple, producing a set of solution mappings.

    Args:
      * triple: RDF triple on which the selection is applied.
      * variables: Input variables of the selection.

    Returns:
      A set of solution mappings built from the selection results.

    Example:
      >>> triple = (":Ann", "foaf:knows", ":Bob")
      >>> variables = ["?s", None, "?knows"]
      >>> selection(triple, variables)
      { "?s": ":Ann", "?knows": ":Bob" }
    """
    bindings = dict()
    if variables[0] is not None:
        bindings[variables[0]] = triple[0]
    if variables[1] is not None:
        bindings[variables[1]] = triple[1]
    if variables[2] is not None:
        bindings[variables[2]] = triple[2]
    return bindings


def find_in_mappings(variable: str, mappings: Dict[str, str] = dict()) -> str:
    """Find a substitution for a SPARQL variable in a set of solution mappings.

    Args:
      * variable: SPARQL variable to look for.
      * bindings: Set of solution mappings to search in.

    Returns:
      The value that can be substituted for this variable.

    Example:
      >>> mappings = { "?s": ":Ann", "?knows": ":Bob" }
      >>> find_in_mappings("?s", mappings)
      ":Ann"
      >>> find_in_mappings("?unknown", mappings)
      "?unknown"
    """
    if not variable.startswith('?'):
        return variable
    return mappings[variable] if variable in mappings else variable


def vars_positions(subject: str, predicate: str, obj: str) -> List[str]:
    """Find the positions of SPARQL variables in a triple pattern.

    Args:
      * subject: Subject of the triple pattern.
      * predicate: Predicate of the triple pattern.
      * obj: Object of the triple pattern.

    Returns:
      The positions of SPARQL variables in the input triple pattern.

    Example:
      >>> vars_positions("?s", "http://xmlns.com/foaf/0.1/name", '"Ann"@en')
      [ "?s", None, None ]
      >>> vars_positions("?s", "http://xmlns.com/foaf/0.1/name", "?name")
      [ "?s", None, "?name" ]
    """
    return [var if var.startswith('?') else None for var in [subject, predicate, obj]]


def tuple_to_triple(s: str, p: str, o: str) -> Dict[str, str]:
    """Convert a tuple-based triple pattern into a dict-based triple pattern.

    Args:
      * s: Subject of the triple pattern.
      * p: Predicate of the triple pattern.
      * o: Object of the triple pattern.

    Returns:
      The triple pattern as a dictionnary.

    Example:
      >>> tuple_to_triple("?s", "foaf:knows", ":Bob")
      { "subject": "?s", "predicate": "foaf:knows", "object": "Bob" }
    """
    return {
        'subject': s,
        'predicate': p,
        'object': o
    }

def md5triple(s:str,p:str,o:str) -> str:
    """create a md5 from a triple
        Args:
          * s: Subject of the triple pattern.
          * p: Predicate of the triple pattern.
          * o: Object of the triple pattern.

        Returns:
          the md5 of s+p+o.
    """
    #print("md5:{},{},{}".format(s,p,o))
    input=s+p+o
    # print("hello:"+tup2str(new_tuple))
    return "http://"+hashlib.md5(input.encode('utf-8')).hexdigest()

def mappings_to_ctx(mappings: Dict[str, str]) -> {}:
    """re-create a rdflib context (ctx) from a sage bag of mappings (grrr)
        Args:
        * mappings: A dictionnary of mappings

        Returns:
        * A context compatible with rdflib

        Ex:
        mappings={'?s': 'http://auth12/scma/s3', '?p': 'http://common/scma/p5', '?o': 'o14'}
        returns:
         {rdflib.term.Variable('s'): rdflib.term.URIRef('http://auth12/scma/s3'),
          rdflib.term.Variable('p'): rdflib.term.URIRef('http://common/scma/p5'),
          rdflib.term.Variable('o'): rdflib.term.Literal('o14')}
    """
    ctx=dict()
    for key,value in mappings.items():
        #print(key+":"+value)
        if (key.startswith('?')):
            key=key[1:]
        else:
            print("mappings_to_ctx found "+key+" as key in mappings")
        if re.match("^http://",value):
            ctx[Variable(key)]=URIRef(value)
        elif re.match("^_:",value):
            ctx[Variable(key)]=BNode(value)
        else:
            ctx[Variable(key)]=Literal(value)
    return ctx
