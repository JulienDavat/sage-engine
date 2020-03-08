#
# For the extension Bind rowid, for contexts
#

See `qbind.sparql` for example

A dummy dataset context.ttl is in `./test/data`
It has been generated with `./tests/data/picontext.pi`
ttl2hdt has been done with rdf2hdt (in java)

Then, SaGe has been extended with a new iterator:
`./sage/query_engine/iterators/bindrow.py`
As a new iterator exist, the saved plan is changed and protobuf has been updated with the new operator
see `./sage/query_engine/protobuf/iterors.proto`. The `iterators_pb2.py`has been regenerated to handle the
presence of the new operator (see https://developers.google.com/protocol-buffers/docs/pythontutorial). Next, the query parser has been modified to recognize the "bind rowid" syntax and insert the BindRowIterator in the generated pipeline. You can see it by:

```
$ python3 explain.py  -q qbind.sparql -d swdf.hdt
http://example.org/rowid
------------
Query
------------
select ?o1 where {
 ?s <http://isa> ?o
 BIND(<http://example.org/rowid>(?s,<http://isa>,?o) as ?z)
 ?z <http://source> ?o1
 }

------------
Algebra
------------
SelectQuery(
    p = Project(
        p = Join(
            p1 = Extend(
                p = BGP(
                    triples = [(rdflib.term.Variable('s'), rdflib.term.URIRef('http://isa'), rdflib.term.Variable('o'))]
                    _vars = {rdflib.term.Variable('o'), rdflib.term.Variable('s')}
                    )
                expr = Function(
                    iri = http://example.org/rowid
                    distinct = []
                    expr = [rdflib.term.Variable('s'), rdflib.term.URIRef('http://isa'), rdflib.term.Variable('o')]
                    _vars = {rdflib.term.Variable('o'), rdflib.term.Variable('s')}
                    )
                var = z
                _vars = {rdflib.term.Variable('z'), rdflib.term.Variable('o'), rdflib.term.Variable('s')}
                )
            p2 = BGP(
                triples = [(rdflib.term.Variable('z'), rdflib.term.URIRef('http://source'), rdflib.term.Variable('o1'))]
                _vars = {rdflib.term.Variable('z'), rdflib.term.Variable('o1')}
                )
            lazy = True
            _vars = {rdflib.term.Variable('z'), rdflib.term.Variable('o'), rdflib.term.Variable('s'), rdflib.term.Variable('o1')}
            )
        PV = [rdflib.term.Variable('o1')]
        _vars = {rdflib.term.Variable('z'), rdflib.term.Variable('o'), rdflib.term.Variable('s'), rdflib.term.Variable('o1')}
        )
    datasetClause = None
    PV = [rdflib.term.Variable('o1')]
    _vars = {rdflib.term.Variable('z'), rdflib.term.Variable('o'), rdflib.term.Variable('s'), rdflib.term.Variable('o1')}
    )
None
extends:
[rdflib.term.Variable('s'), rdflib.term.URIRef('http://isa'), rdflib.term.Variable('o')]
z
['?s', 'http://isa', '?o']
Join P1 _vars
{rdflib.term.Variable('z'), rdflib.term.Variable('o'), rdflib.term.Variable('s')}
-----------------
Iterator pipeline
-----------------
<ProjectionIterator SELECT ['?o1'] FROM <IndexJoinIterator (<BindRowIdIterator BIND ['?s', 'http://isa', '?o'] AS ?z FROM <ScanIterator (?s http://isa ?o)>> JOIN { ?z http://source ?o1 })>>
-----------------
Cardinalities
-----------------
[]
```

Modifying the query_parser occurs in `./sage/optimizer/query_parser.py`. 
The parser by itself is unchanged, but the "builder" (see https://en.wikipedia.org/wiki/Builder_pattern) has been modified
see function "parse_query_alt" and compare to the previous one "parse_query_node". The problem to solve was to handle new "extend" nodes in the algebra tree. Binds introduce
 orders in the evaluation of patterns ie. binded variables must exist before being used. We see that in the  tree: JOIN.p1 first, then Join.P2. The "join" part of the builder has been rewritten calling 'continue_left_join_tree' for any right BGP. Then, (i hope that), only the
leaf left BGP is activated by the 'BGP' builder -> build_left_join_tree. The general idea is that join operators continue the left linear tree initiated by the leaf left BGP.

There is a drawback:
* Join ordering can be non-optimal -> not the most selective pattern left.
* I broke the OLD join code managing several BGP from different graph... (can be reinserted, but need to be tested)


Next several tests has been added to check (can be run with "pytest ./tests/iterators/rowid_iterator_test.py"):
* `./tests/iterators/rowid_iterator_test.py` (check the iterator itselft)
* `./tests/optimizer/parse_rowid_test.py` (check if the right pipeline is generated from the query)
* `./tests/http/rowid_interface_test.py` (check if the query returns the right results)

