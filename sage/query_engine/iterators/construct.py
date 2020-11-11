# construct.py
# Author: Pascal Molli - MIT License 2017-2020

from rdflib import BNode, Literal, URIRef, Variable
from rdflib import Graph
from typing import Dict, List, Optional

from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.protobuf.iterators_pb2 import (SavedConstructIterator,TriplePattern)
from sage.query_engine.iterators.utils import find_in_mappings
from sage.query_engine.iterators.filter import to_rdflib_term

def convert_construct_template(template):
    result=[]
    for triple_pattern in template:
        tp=[]
        for term in triple_pattern:
            if isinstance(term,Variable):
                tp.append(term.n3())
            else:
                tp.append(str(term))
        result.append((tp[0],tp[1],tp[2]))
    return result

class ConstructIterator(PreemptableIterator):
    """A ConstructIterator evaluates a SPARQL construct (CONSTRUCT) in a pipeline of iterators.

    Args:
      * source: Previous iterator in the pipeline.
      * template: Construct template: a list of of triple of RDF term
    """

    def __init__(self, source: PreemptableIterator, template: List[str] = None):
        super(ConstructIterator, self).__init__()
        self._source = source
        self._template=template
        self._graph=Graph()

    def graph(self) -> Graph:
        return self._graph

    def __repr__(self) -> str:
        return f"<ConstructIterator CONSTRUCT {self._template} FROM {self._source}>"

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "construct"

    def has_next(self) -> bool:
        """Return True if the iterator has more item to yield"""
        return self._source.has_next()

    def next_stage(self, binding: Dict[str, str]):
        self._source.next_stage(binding)

    async def next(self) -> Optional[Dict[str, str]]:
        """Get the next item from the iterator, following the iterator protocol.

        This function may contains `non interruptible` clauses which must
        be atomically evaluated before preemption occurs.

        Returns: A set of solution mappings, or `None` if none was produced during this call.

        Throws: `StopAsyncIteration` if the iterator cannot produce more items.
        """
        if not self.has_next():
            raise StopAsyncIteration()
        mappings = await self._source.next()
        if mappings is None:
            return None

        # itemp = instanciated template, list of triple
        for triple in self._template:
            bounded_triple = []
            for term in triple:
                if term.startswith('?'):
                    bounded_triple.append(to_rdflib_term(find_in_mappings(term,mappings)))
                else:
                    bounded_triple.append(to_rdflib_term(term))
            #line=bounded_triple[0]+" "+bounded_triple[1]+" "+bounded_triple[2]+" . "
            # for i in range(0, len(bounded_triple)):
            #     if bounded_triple[i].startswith("http"):
            #         bounded_triple[i]=URIRef(bounded_triple[i])
            #     elif bounded_triple[i].startswith("_:"):
            #         bounded_triple[i]=BNode(bounded_triple[i])
            #     else:
            #         bounded_triple[i]=Literal(str(bounded_triple[i]))
            #self._graph.parse(data=line, format='nt')
            self._graph.add( (bounded_triple[0],bounded_triple[1],bounded_triple[2]) )
        return None

    def save(self) -> SavedConstructIterator:
        """Save and serialize the iterator as a Protobuf message"""
        saved_constr = SavedConstructIterator()
        source_field = self._source.serialized_name() + '_source'
        getattr(saved_constr, source_field).CopyFrom(self._source.save())

        tp_list=[]
        for tp in self._template:
            tp_save=TriplePattern()
            tp_save.subject=tp[0]
            tp_save.predicate=tp[1]
            tp_save.object=tp[2]
            tp_save.graph=""
            tp_list.append(tp_save)
        saved_constr.template.extend(tp_list)
        #print("construct_save:"+str(saved_constr))
        return saved_constr
