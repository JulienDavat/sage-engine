# commons.py
# Author: Thomas MINIER - MIT License 2017-2019
# Author: Pascal Molli - MIT License 2017-2019

from rdflib import BNode, Literal, URIRef, Variable
from rdflib import Graph

from starlette.testclient import TestClient
from tests.http.utils import post_sparql

from sage.http_server.server import run_app
from sage.query_engine.optimizer.query_parser import parse_query
from sage.database.core.yaml_config import load_config
from sage.query_engine.sage_engine import SageEngine

import click
import requests
from json import dumps
from math import inf
from sys import exit
import json
import asyncio

import coloredlogs
import logging
from time import time

# install logger
coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


@click.command()
@click.argument("entrypoint")
@click.argument("default_graph_uri")
@click.option("-q", "--query", type=str, default=None, help="SPARQL query to execute (passed in command-line)")
@click.option("-f", "--file", type=str, default=None, help="File containing a SPARQL query to execute")
@click.option("-l", "--limit", type=int, default=None, help="Maximum number of solutions bindings to fetch, similar to the SPARQL LIMIT modifier.")
@click.option("--format", type=click.Choice(["n3", "nquads","nt","pretty-xml","trig","trix","turtle","xml"]), default="nt", help="Format of the results set, formatted according to W3C SPARQL standards.")
def sage_client_construct(entrypoint, default_graph_uri, query, file, limit,format):
    """
        Send a SPARQL query to a SaGe server hosted at ENTRYPOINT, with DEFAULT_GRAPH_URI as the default RDF Graph. It does not act as a Smart client, so only queries supported by the server will be evaluated.

        Example usage: sage-client http://sage.univ-nantes.fr/sparql http://sage.univ-nantes.fr/sparql/dbpedia-2016-04 -q "SELECT * WHERE { ?s ?p ?o }"
    """
    # assert that we have a query to evaluate
    if query is None and file is None:
        print("Error: you must specificy a query to execute, either with --query or --file. See sage-query --help for more informations.")
        exit(1)

    if limit is None:
        limit = inf

    # load query from file if required
    if file is not None:
        with open(file) as query_file:
            query = query_file.read()

    # prepare query headers
    headers = {
        "accept": "text/html",
        "content-type": "application/json",
        "next": None
    }
    # TODO support xml
    # if format == "xml":
    #     headers["Accept"] = "application/sparql-results+xml"

    payload = {
        "query": query,
        "defaultGraph": default_graph_uri,
    }
    has_next = True
    count = 0
    nbResults = 0
    nbCalls = 0

    g=Graph()

    start=time()
    while has_next:
        response = requests.post(entrypoint, headers=headers, data=dumps(payload))
        json_response = response.json()
        has_next = json_response['next']
        payload["next"] = json_response["next"]
        nbResults += len(json_response['bindings'])
        nbCalls += 1

        buffer=""
        for triple in json_response['bindings']:
            line=triple['s']+" "+triple['p']+" "+triple['o']+" . \n"
            buffer=buffer+line
        g.parse(data=buffer, format='nt')
        logger.info("{} calls, {} triples, {} triples in graph".format(nbCalls,nbResults,len(g)))
        if len(g)>=limit:
            break

    end=time()
    logger.info("finished in {}s".format(end-start))
    logger.info("made {} calls".format(nbCalls))
    logger.info("got {} mappings".format(nbResults))
    logger.info("gathered a graph of {} triples".format(len(g)))
    print(g.serialize(format=format).decode('utf8'))


@click.command()
@click.argument("config_file")
@click.argument("default_graph_uri")
@click.option("-q", "--query", type=str, default=None, help="SPARQL query to execute (passed in command-line)")
@click.option("-f", "--file", type=str, default=None, help="File containing a SPARQL query to execute")
@click.option("--format", type=click.Choice(["n3", "nquads","nt","pretty-xml","trig","trix","turtle","xml"]), default="nt", help="Format of the results set, formatted according to W3C SPARQL standards.")
@click.option("-l", "--limit", type=int, default=None, help="Maximum number of solutions bindings to fetch, similar to the SPARQL LIMIT modifier.")
def sage_query_construct(config_file, default_graph_uri, query, file, format, limit):
    """
        Execute a SPARQL CONSTRUCT query to a SaGe server.

        Example usage: sage-construct config.yaml http://example.org/swdf-postgres -f queries/construct.sparql
    """
    # assert that we have a query to evaluate
    if query is None and file is None:
        print("Error: you must specificy a query to execute, either with --query or --file. See sage-query --help for more informations.")
        exit(1)

    if limit is None:
        limit = inf

    # load query from file if required
    if file is not None:
        with open(file) as query_file:
            query = query_file.read()

    client=TestClient(run_app(config_file))


    nbResults = 0
    nbCalls = 0
    hasNext = True
    next_link = None

    g=Graph()
    start = time()

    while hasNext:
        response = post_sparql(client, query, next_link, default_graph_uri)
        response = response.json()
        nbResults += len(response['bindings'])
        hasNext = response['hasNext']
        next_link = response['next']
        nbCalls += 1
        buffer=""
        for triple in response['bindings']:
            line=triple['s']+" "+triple['p']+" "+triple['o']+" . \n"
            buffer=buffer+line
        g.parse(data=buffer, format='nt')
        logger.info("{} calls, {} triples, {} triples in graph".format(nbCalls,nbResults,len(g)))
        if len(g)>=limit:
            break


    end= time()
    logger.info("finished in {}s".format(end-start))
    logger.info("made {} calls".format(nbCalls))
    logger.info("got {} triples".format(nbResults))
    logger.info("gathered a graph of {} triples".format(len(g)))

    print(g.serialize(format=format).decode('utf8'))
