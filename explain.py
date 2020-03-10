#!/usr/bin/python

from sage.cli.utils import load_graph
from sage.database.core.yaml_config import load_config

from rdflib.plugins.sparql.algebra import translateQuery, translateUpdate
from rdflib.plugins.sparql.parser import parseQuery, parseUpdate
from rdflib.plugins.sparql.algebra import pprintAlgebra
from rdflib.plugins.sparql.parserutils import prettify_parsetree
# seems that register custom function not present in rdflib 4.2.2
# only on very last version
from rdflib.plugins.sparql.operators import register_custom_function
from rdflib import BNode, Graph, Literal, Namespace, RDFS, XSD
import inspect
import click
import coloredlogs
import logging
import asyncio

# be sure to load what i beleive ;)
#print(inspect.getfile(register_custom_function))
#print(inspect.getfile(parseQuery))

from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.optimizer.query_parser import parse_query
import math
import pprint


def rowid(x,y,z):
    return Literal("%s %s %s" % (x, y,z), datatype=XSD.string)

async def execute(iterator):
    try:
        while iterator.has_next():
            value = await iterator.next()
            # discard null values
            if value is not None:
                print(value)
    except StopAsyncIteration:
#        print("stop")
        pass


@click.command()
@click.argument("query_file")
@click.argument("config_file")
@click.argument("graph_uri")
@click.option("-u", "--update", is_flag=True, help="explain a SPARQL update query")
@click.option("-p", "--parse", is_flag=True, help="print the query parse tree")
@click.option("-i", "--indentnb", default=2, help="pretty print indent value")
def explain(query_file,config_file,graph_uri,indentnb,update,parse):
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    dataset = load_config(config_file)
    if dataset is None:
        print("config file {config_file} not found")
        exit(1)


    graph = dataset.get_graph(graph_uri)
    if graph is None:
        print("RDF Graph  not found:"+graph_uri)
        exit(1)

    engine = SageEngine()
    pp = pprint.PrettyPrinter(indent=indentnb)

    query='';
    with open(query_file,'r') as f:
        query = f.read()

    if query is None:
        exit(1)

    SAGE = Namespace('http://example.org/')
    print(SAGE.rowid)
    register_custom_function(SAGE.rowid, rowid)

    print("------------")
    print("Query")
    print("------------")
    print(query)


    if update:
        pq=parseUpdate(query)
    else:
        pq=parseQuery(query)

    if pq is None:
        exit(1)

    if parse:
        print("------------")
        print("Parsed Query")
        print("------------")
        pp.pprint(pq)
        print(prettify_parsetree(pq))

    if update:
        tq=translateUpdate(pq)
    else:
        tq = translateQuery(pq)
    print("------------")
    print("Algebra")
    print("------------")
    print(pprintAlgebra(tq))

    #logical_plan = tq.algebra
    cards = list()

    iterator,cards = parse_query(query, dataset, graph_uri)


    print("-----------------")
    print("Iterator pipeline")
    print("-----------------")
    print(iterator)
    print("-----------------")
    print("Cardinalities")
    print("-----------------")
    pp.pprint(cards)


    print("-----------------")
    print("Results")
    print("-----------------")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(execute(iterator))
    loop.close()

    # discard null values

    # quota = graph.quota / 1000
    # max_results = graph.max_results
    # bindings, saved_plan, is_done, abort_reason = await engine.execute(iterator, quota, max_results)
    # print(str(bindings))


if __name__ == '__main__':
    explain()
