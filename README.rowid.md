#
# For the extension Bind rowid, for contexts
#

See `qbind.sparql` for example

A dummy dataset context.md5.ttl is in `./test/data`
It has been generated with query `./tests/data/contextgen.sparql`
ttl2hdt has been done with rdf2hdt (in java)

Then, SaGe has been extended with a new iterator:
`./sage/query_engine/iterators/bind.py`
As a new iterator exist, the saved plan is changed and protobuf has been updated with the new operator
see `./sage/query_engine/protobuf/iterors.proto`. The `iterators_pb2.py`has been regenerated to handle the
presence of the new operator (see https://developers.google.com/protocol-buffers/docs/pythontutorial). Next, the query parser has been modified to recognize the "bind" syntax and insert the BindIterator in the generated pipeline. You can see it by:

```
$ python3 explain.py  queries/bind.sparql ./tests/data/test_config.yaml http://localhost:8000/sparql/context
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
* `./tests/iterators/bind_iterator_test.py` (check the iterator itselft)
* `./tests/optimizer/bind_parse_test.py` (check if the right pipeline is generated from the query)
* `./tests/http/bind_interface_test.py` (check if the query returns the right results)
* `./test/update/bind_test.py` (check insert queries)
