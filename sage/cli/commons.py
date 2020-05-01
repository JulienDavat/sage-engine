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

from sage.query_engine.iterators.loader import load
from sage.query_engine.optimizer.query_parser import parse_query
from sage.query_engine.sage_engine import SageEngine
from sage.http_server.utils import decode_saved_plan, encode_saved_plan
from sage.database.core.dataset import Dataset
from sage.database.core.yaml_config import load_config

from sage.query_engine.protobuf.iterators_pb2 import (RootTree,
                                                      SavedBagUnionIterator,
                                                      SavedFilterIterator,
                                                      SavedIndexJoinIterator,
                                                      SavedProjectionIterator,
                                                      SavedReducedIterator,
                                                      SavedScanIterator,
                                                      SavedBindIterator,
                                                      SavedConstructIterator)

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
def sage_client(entrypoint, default_graph_uri, query, file, limit):
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

    start=time()
    while has_next and count < limit:
        response = requests.post(entrypoint, headers=headers, data=dumps(payload))
        json_response = response.json()
        has_next = json_response['next']
        payload["next"] = json_response["next"]
        nbResults += len(json_response['bindings'])
        nbCalls += 1
        for bindings in json_response['bindings']:
            print(str(bindings))
        count += 1
        if count >= limit:
            break
    end=time()
    logger.info("finished in {}s".format(end-start))
    logger.info("made {} calls".format(nbCalls))
    logger.info("got {} mappings".format(nbResults))


def progress(saved_plan):
    try:
        #print(f"...{type(saved_plan)}...")
        if type(saved_plan) is SavedScanIterator:
            return saved_plan.progress,saved_plan.cardinality
        elif type(saved_plan) is SavedBagUnionIterator:
            sourceField=saved_plan.WhichOneof('left')
            return progress(getattr(saved_plan,sourceField))
        else:
            sourceField=saved_plan.WhichOneof('source')
            return progress(getattr(saved_plan, sourceField))
    except:
        logger.warning(f"progress {type(saved_plan)} has no source")
        #exit()
        return 1,1


@click.command()
@click.argument("config_file")
@click.argument("default_graph_uri")
@click.option("-q", "--query", type=str, default=None, help="SPARQL query to execute (passed in command-line)")
@click.option("-f", "--file", type=str, default=None, help="File containing a SPARQL query to execute")
@click.option("-l", "--limit", type=int, default=None, help="Maximum number of solutions bindings to fetch, similar to the SPARQL LIMIT modifier.")
def sage_query(config_file, default_graph_uri, query, file, limit):
    """
        Execute a SPARQL query on an embedded Sage Server.

        Example usage: sage-query config.yaml http://example.org/swdf-postgres -f queries/spo.sparql
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

    # dataset = load_config(config_file)
    # if not dataset.has_graph(default_graph_uri):
    #     print("Error: the config_file does not define your {default_graph_uri}.")
    client=TestClient(run_app(config_file))

    nbResults = 0
    nbCalls = 0
    hasNext = True
    next_link = None
    count=0
    start = time()

    while hasNext:
        response = post_sparql(client, query, next_link, default_graph_uri)
        response = response.json()
        nbResults += len(response['bindings'])
        hasNext = response['hasNext']
        next_link = response['next']


        nbCalls += 1
        for bindings in response['bindings']:
            print(bindings)
#            for k,v in bindings.items():
#                print(f"{v} ")

        if next_link is not None:
            saved_plan = next_link
            plan = decode_saved_plan(saved_plan)
            root = RootTree()
            root.ParseFromString(plan)
            prog,card=progress(root)
            logger.info(f"progression {prog}/{card}:{prog/card*100}%")


        count += 1
        if count >= limit:
            break

    end= time()
    logger.info("finished in {}s".format(end-start))
    logger.info("made {} calls".format(nbCalls))
    logger.info("got {} mappings".format(nbResults))
