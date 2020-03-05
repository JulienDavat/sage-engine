#!/usr/bin/python
from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate
from rdflib.plugins.sparql.algebra import pprintAlgebra
from rdflib.plugins.sparql.parserutils import prettify_parsetree
# seems that register custom function not present in rdflib 4.2.2
# only on very last version
from rdflib.plugins.sparql.operators import register_custom_function
from rdflib import BNode, Graph, Literal, Namespace, RDFS, XSD
import inspect

# be sure to load what i beleive ;)
#print(inspect.getfile(register_custom_function))
#print(inspect.getfile(parseQuery))

from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.optimizer.query_parser import parse_query,parse_query_node
from sage.database.hdt.connector import HDTFileConnector
from tests.utils import DummyDataset
import math
import pprint

import sys,getopt

def rowid(x,y,z):
    return Literal("%s %s %s" % (x, y,z), datatype=XSD.string)


def explain(query,hdt):
    hdtDoc = HDTFileConnector(hdt)
    dataset = DummyDataset(hdtDoc, 'dummy')
    engine = SageEngine()
    pp = pprint.PrettyPrinter(indent=4)

    SAGE = Namespace('http://example.org/')
    print(SAGE.rowid)
    register_custom_function(SAGE.rowid, rowid)

    print("------------")
    print("Query")
    print("------------")
    print(query)



    pq=parseUpdate(query)
#    print("------------")
#    print("Parsed Query")
#    print("------------")
#    pp.pprint(pq)
#    print(prettify_parsetree(pq))


    tq = translateUpdate(pq)
    print("------------")
    print("Algebra")
    print("------------")
    print(pprintAlgebra(tq))

    #logical_plan = tq.algebra
    cardinalities = list()

    iterator,cards = parse_query(query, dataset, 'context')


    print("-----------------")
    print("Iterator pipeline")
    print("-----------------")
    print(iterator)
    print("-----------------")
    print("Cardinalities")
    print("-----------------")
    pp.pprint(cardinalities)

    # return iterator, cardinalities
    # iterator, cards = parse_query(query, dataset, 'watdiv100')
    # assert len(cards) > 0
    # assert iterator is not None


def main(argv):
    query = ''
    hdt = ''
    try:
        opts, args = getopt.getopt(argv,"hq:d:",["qfile=","dfile="])
    except getopt.GetoptError:
        print('explain-update.py -q <query> -d <hdtfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('explain-update.py -q <query> -d <hdtfile>')
            sys.exit()
        elif opt in ("-q", "--query"):
            with open(arg,'r') as f:
                query = f.read()
        elif opt in ("-d", "--data"):
            hdt = arg
    explain(query,hdt)

if __name__ == "__main__":
   main(sys.argv[1:])
