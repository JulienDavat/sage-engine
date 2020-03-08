# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2020
import pytest
import asyncio

from datetime import datetime

from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate


from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.bindrow import BindRowIterator
from sage.query_engine.iterators.bindrowsource import BindRowSourceIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.utils import EmptyIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.database.hdt.connector import HDTFileConnector

from sage.query_engine.optimizer.query_parser import parse_query

from tests.utils import DummyDataset

hdtDoc = HDTFileConnector('tests/data/context.hdt')
engine = SageEngine()
triple = {
    'subject': '?s',
    'predicate': 'http://isa',
    'object': '?o',
    'graph': 'context'
}

innerTriple = {
    'subject': '?z',
    'predicate': 'http://source',
    'object': '?o1',
    'graph': 'context'
}

# Run the equivalent of
#    select * where {
#      ?s <http://isa> ?o
#    }
@pytest.mark.asyncio
async def test_scan_read():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    (results, saved, done, _) = await engine.execute(scan, 10e7)
    print(results)
    assert len(results) > 0
    assert done



# Run the equivalent of
#    select * where {
#      ?z <http://source> ?o1
#    }
@pytest.mark.asyncio
async def test_scan_inner():
    iterator, card = hdtDoc.search(innerTriple['subject'], innerTriple['predicate'], innerTriple['object'])
    scan = ScanIterator(iterator, innerTriple, card)
    (results, saved, done, _) = await engine.execute(scan, 10e7)
    # while scan.has_next():
    #     value =  await scan.next()
    #     print(value)
    assert len(results) > 0
    assert done


# Run the equivalent of
#    select * where {
#      ?s <http://isa> ?o
#      BIND(<http://example.org/rowid>(?s,<http://isa>,?o) as ?z)
#    }
@pytest.mark.asyncio
async def test_rowbind():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan=ScanIterator(iterator, triple, card)
    rowbind=BindRowIterator(scan,['?s','http://isa','?o'],'?z')
    print(rowbind)

    (results, saved, done, _) = await engine.execute(rowbind, 10e7)
    # while rowbind.has_next():
    #     value =  await rowbind.next()
    #     print(value)
    assert len(results) > 0
    assert done



# Run the equivalent of
#    select * where {
#      ?s <http://isa> ?o
#      BIND(<http://example.org/rowid>(?s,<http://isa>,?o) as ?z)
#      ?z <http://source> ?o1
#    }
@pytest.mark.asyncio
async def test_rowbind_join():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan=ScanIterator(iterator, triple, card)
    rowbind=BindRowIterator(scan,['?s','http://isa','?o'],'?z')
    join=IndexJoinIterator(rowbind,innerTriple,hdtDoc)

    print(join)

    (results, saved, done, _) = await engine.execute(join, 10e7)
    print(results)
    assert len(results) > 0
    assert done



# Run the equivalent of
#    select ?z where {
#      ?s <http://isa> ?o
#      BIND(<http://example.org/rowid>(?s,<http://isa>,?o) as ?z)
#      ?z <http://source> ?o1
#    }
@pytest.mark.asyncio
async def test_rowbind_join_proj():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan=ScanIterator(iterator, triple, card)
    rowbind=BindRowIterator(scan,['?s','http://isa','?o'],'?z')
    join=IndexJoinIterator(rowbind,innerTriple,hdtDoc)
    proj=ProjectionIterator(join,['?z'])

    print(proj)

    (results, saved, done, _) = await engine.execute(proj, 10e7)
    print(results)
    assert len(results) > 0
    assert done

# Run the equivalent of
#    select ?z where {
#      ?s <http://isa> ?o
#      BIND(<http://example.org/rowid>(?s,<http://isa>,?o) as ?z)
#      ?z <http://source> ?o1
#    }
@pytest.mark.asyncio
async def test_rowbind_empty():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    rbs=BindRowSourceIterator(['http://donald','http://isa','connard'],'?z')

    print(rbs)

    (results, saved, done, _) = await engine.execute(rbs, 10e7)
    print(results)
    assert len(results) > 0
    assert done



# loop = asyncio.get_event_loop()
# loop.run_until_complete(test_scan_inner())
# loop.run_until_complete(test_rowbind())
# loop.run_until_complete(test_rowbind_join())
# loop.run_until_complete(test_rowbind_join_proj())
# loop.close()
