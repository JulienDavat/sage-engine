# scan_test.py
# Author: Thomas MINIER - MIT License 2017-2020
import pytest
import asyncio
from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.bindrow import BindRowIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.database.hdt.connector import HDTFileConnector

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

@pytest.mark.asyncio
async def test_scan_read():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan = ScanIterator(iterator, triple, card)
    (results, saved, done, _) = await engine.execute(scan, 10e7)
    print(results)
    assert len(results) > 0
    assert done


# loop = asyncio.get_event_loop()
# loop.run_until_complete(test_scan_read())
# loop.close()

async def test_scan_inner():
    iterator, card = hdtDoc.search(innerTriple['subject'], innerTriple['predicate'], innerTriple['object'])
    scan = ScanIterator(iterator, innerTriple, card)
    while scan.has_next():
        value =  await scan.next()
        print(value)

async def test_rowbind():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan=ScanIterator(iterator, triple, card)
    rowbind=BindRowIterator(scan,['?s','http://isa','?o'],'?z')
    print(rowbind)
    while rowbind.has_next():
        value =  await rowbind.next()
        print(value)

async def test_rowbind_join():
    iterator, card = hdtDoc.search(triple['subject'], triple['predicate'], triple['object'])
    scan=ScanIterator(iterator, triple, card)
    rowbind=BindRowIterator(scan,['?s','http://isa','?o'],'?z')
    join=IndexJoinIterator(rowbind,innerTriple,hdtDoc)

    print(join)

    (results, saved, done, _) = await engine.execute(join, 10e7)
    print(results)



loop = asyncio.get_event_loop()
loop.run_until_complete(test_scan_inner())
loop.run_until_complete(test_rowbind())
loop.run_until_complete(test_rowbind_join())
loop.close()
