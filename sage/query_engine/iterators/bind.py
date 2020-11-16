# projection.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, List, Optional, Tuple

from rdflib.plugins.sparql.evalutils import _eval
from rdflib import BNode, Literal, URIRef, Variable
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.sparql import Bindings, QueryContext
from rdflib.util import from_n3


from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.primitives import PreemptiveLoop
from sage.query_engine.iterators.utils import find_in_mappings, EmptyIterator
from sage.query_engine.protobuf.iterators_pb2 import SavedBindIterator
from sage.query_engine.protobuf.utils import pyDict_to_protoDict
from sage.query_engine.iterators.filter import to_rdflib_term

from rdflib.plugins.sparql.operators import register_custom_function

from urllib.parse import urlparse
import codecs

class BindIterator(PreemptableIterator):
    """A BindIterator evaluates a BIND statement in a pipeline of iterators.

    Args:
      * source: Previous iterator in the pipeline.
      * expression: a text representing the bind expression
      * bindvar: the bind variable
    """

    def __init__(self, source: PreemptableIterator, bindexpr: str, bindvar: str, mu: Optional[Dict[str, str]] = None, delivered: Optional[bool] = False):
        super(BindIterator, self).__init__()
        self._source = source
        self._expr=bindexpr
        self._bindvar = bindvar
        self._mu = mu
        self._delivered=delivered
        #print("bindexpr:"+bindexpr)
        #print("bindvar:"+bindexpr)

        compiled_expr = parseQuery(f"SELECT * WHERE {{?s ?p ?o . BIND({bindexpr} as {bindvar})}}")
        compiled_expr = translateQuery(compiled_expr)
        self._prologue = compiled_expr.prologue
        #print("bind_compil:"+str(compiled_expr.algebra.p.p.expr))
        #self._compiled_expression = compiled_expr.algebra.p.p.expr
        self._compiled_expression = compiled_expr.algebra.p.p.expr

    def __len__(self) -> str:
        """Get an approximation of the result's cardinality of the iterator"""
        return self._source.__len__()

    def __repr__(self) -> str:
        return f"<BindIterator BIND {self._expr} AS {self._bindvar} FROM {self._source}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "bind"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        if self._source is None:
            return not self._delivered
        else:
            return self._mu is not None or self._source.has_next()

    def next_stage(self, binding: Dict[str, str]):
        self._source.next_stage(binding)

    def _evaluate(self, bindings: Dict[str, str]) -> bool:
        """Evaluate the BIND expression with a set mappings.

        Argument: A set of solution mappings.

        Returns: The outcome of evaluating the SPARQL BIND on the input set of solution mappings.
        """
        context = None
        if bindings is None:
            context=QueryContext(Bindings())
        else:
            d = {Variable(key[1:]): to_rdflib_term(value) for key, value in bindings.items()}
            b = Bindings(d=d)
            context = QueryContext(bindings=b)
        context.prologue = self._prologue
        self._result = self._compiled_expression.eval(context)
        return self._result

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if not self.has_next():
            return None
        elif self._source is None:
            mappings = dict()
            mappings[self._bindvar] = str(self._evaluate(self._mu))
            self._delivered = True
            return mappings
        with PreemptiveLoop() as loop:
            while self._mu is None or not self._evaluate(self._mu):
                self._mu = await self._source.next()
                if self._mu is None:
                    return None
                await loop.tick()
        mu = self._mu
        mu[self._bindvar] = str(self._result)
        self._mu = None
        return mu

        # if not self.has_next():
        #     raise StopAsyncIteration()

        # if self._source is None:
        #     mappings=dict()
        #     mappings[self._bindvar]=str(self._evaluate(self._mu))
        #     self._delivered=True
        #     return mappings
        # else:
        #     if self._mu is None:
        #         self._mu = await self._source.next()
        #     with PreemptiveLoop() as loop:
        #         while not self._evaluate(self._mu):
        #             self._mu = await self._source.next()
        #             await loop.tick()
        #     if not self.has_next():
        #         raise StopAsyncIteration()
        #     mu = self._mu
        #     mu[self._bindvar]=str(self._result)
        #     self._mu = None
        #     return mu

    def save(self) -> SavedBindIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_bind = SavedBindIterator()
        if self._source is not None:
            source_field = self._source.serialized_name() + '_source'
            getattr(saved_bind, source_field).CopyFrom(self._source.save())
        saved_bind.bindexpr= self._expr
        saved_bind.bindvar=self._bindvar
        if self._mu is not None:
            pyDict_to_protoDict(self._mu, saved_bind.mu)
        saved_bind.delivered=self._delivered
        return saved_bind
