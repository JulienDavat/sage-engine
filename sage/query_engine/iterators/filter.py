# filter.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Optional, Union, List

from rdflib import Literal, URIRef, Variable, BNode
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.sparql import Bindings, QueryContext
from rdflib.util import from_n3,to_term
from rdflib.plugins.parsers.ntriples import unquote,uriquote
from rdflib.term import _is_valid_uri

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.primitives import PreemptiveLoop
from sage.query_engine.protobuf.iterators_pb2 import SavedFilterIterator
from sage.query_engine.protobuf.utils import pyDict_to_protoDict

import logging
logger = logging.getLogger(__name__)
import warnings
import sys

def to_rdflib_term(value: str) -> Union[Literal, URIRef, Variable, BNode]:
    """Convert a N3 term to a RDFLib Term.

    Argument: A RDF Term in N3 format.

    Returns: The RDF Term in rdflib format.
    """
# Tbe real pb:
# From variable -> we create mappings by reading in the base
# however:
#  - We dont remember if it was a URI, Bnode, or litteral
#  - And when we want to process filters, or bindings with RDFLib
# we have to convert strings to RDFTerm in order to process them with
# rdflib evaluation functions

    ## if data has been ingested with postgres sput !!
    if value.startswith('"'):
        try:
            return from_n3(value)
        except:
            return Literal(f"{sys.exc_info()[0]}")
    elif value.startswith('_'):
        return BNode(value)
    elif value.startswith('http'):
        return URIRef(value)
    else:
        return Literal(value)

    # if value.startswith('http') or value.startswith('file') or value.startswith('mailto'):
    #     return URIRef(value)
    # #managing Literals
    # #"That Seventies Show"^^<http://www.w3.org/2001/XMLSchema#string>
    # # generate N3 repr and parse...
    # result=None
    # try :
    #     if value.startswith('"'):
    #         result=from_n3(value)
    #     else:
    #         result=from_n3('"'+value+'"')
    # except:
    #     logger.warning(f'to_rdflib_term: {value} cannot be converted to RDF term. reason: {sys.exc_info()[0]}')
    #     result=Literal(value.encode('utf-8','replace').decode('utf-8'))
    # return result


class FilterIterator(PreemptableIterator):
    """A FilterIterator evaluates a FILTER clause in a pipeline of iterators.

    Args:
      * source: Previous iterator in the pipeline.
      * expression: A SPARQL FILTER expression.
      * mu: Last set of mappings read by the iterator.
    """

    def __init__(self, source: PreemptableIterator, expression: str, mu: Optional[Dict[str, str]] = None):
        super(FilterIterator, self).__init__()
        self._source = source
        self._raw_expression = expression
        self._mu = mu
        # compile the expression using rdflib
        compiled_expr = parseQuery(f"SELECT * WHERE {{?s ?p ?o . FILTER({expression})}}")
        compiled_expr = translateQuery(compiled_expr)
        self._prologue = compiled_expr.prologue
        self._compiled_expression = compiled_expr.algebra.p.p.expr

    def __repr__(self) -> str:
        return f"<FilterIterator '{self._raw_expression}' on {self._source}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "filter"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._mu is not None or self._source.has_next()

    def next_stage(self, binding: Dict[str, str]):
        self._source.next_stage(binding)

    def _evaluate(self, bindings: Dict[str, str]) -> bool:
        """Evaluate the FILTER expression with a set mappings.

        Argument: A set of solution mappings.

        Returns: The outcome of evaluating the SPARQL FILTER on the input set of solution mappings.
        """
        d = {Variable(key[1:]): to_rdflib_term(value) for key, value in bindings.items()}
        b = Bindings(d=d)
        context = QueryContext(bindings=b)
        context.prologue = self._prologue
        return self._compiled_expression.eval(context)

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if not self.has_next():
            return None
        with PreemptiveLoop() as loop:
            while self._mu is None or not self._evaluate(self._mu):
                self._mu = await self._source.next()
                if self._mu is None:
                    return None
                await loop.tick()
        mu = self._mu
        self._mu = None
        return mu

        # if not self.has_next():
        #     raise StopAsyncIteration()
        # if self._mu is None:
        #     self._mu = await self._source.next()
        # with PreemptiveLoop() as loop:
        #     while not self._evaluate(self._mu):
        #         self._mu = await self._source.next()
        #         await loop.tick()
        # if not self.has_next():
        #     raise StopAsyncIteration()
        # mu = self._mu
        # self._mu = None
        # return mu

    def save(self) -> SavedFilterIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_filter = SavedFilterIterator()
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_filter, source_field).CopyFrom(self._source.save())
        saved_filter.expression = self._raw_expression
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_filter.mu)
        return saved_filter
