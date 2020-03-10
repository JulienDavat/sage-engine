# postgres.py
# Author: Thomas MINIER - MIT License 2017-2019
import sage.cli.postgres_utils as p_utils
from sage.cli.utils import load_graph, get_rdf_reader
from sage.query_engine.iterators.utils import md5triple
from rdflib import Graph, BNode, Literal, URIRef, Variable
import click
import coloredlogs
import logging
from time import time
import string


@click.command()
@click.argument("rdf_file")
@click.argument("graph_name")
@click.option("-f", "--format", type=click.Choice(["nt", "ttl"]),
              default="nt", show_default=True, help="Format of the input file. Supported: nt (N-triples), ttl (Turtle) and hdt (HDT).")
@click.option("-c", "--context",
              default="http://source", show_default=True, help="string representing the context relation")
def contextualize(graph_name, rdf_file, format, context):
    """
     contextualize every triples from RDF_file with a context relation with graph_name as object.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)


    logger.info("Reading RDF source file...")

    iterator=None
    nb_triples=0
    #iterator, nb_triples = get_rdf_reader(rdf_file, format=format)
    if format == 'nt' or format == 'ttl':
        g = Graph()
        g.parse(rdf_file, format=format)
        nb_triples = len(g)
        iterator = g.triples((None, None, None))

    logger.info("RDF source file loaded. Found ~{} RDF triples to ingest.".format(nb_triples))

    # insert rdf triples
    start = time()
    # insert by bucket (and show a progress bar)
    with click.progressbar(length=nb_triples,
                           label="Inserting RDF triples".format(nb_triples)) as bar:
        for s, p, o in iterator:
            print(s.n3()+" "+p.n3()+" "+o.n3()+" .")
            if isinstance(o,Literal):
                print("<"+md5triple(str(s),str(p),o.n3())+"> <"+context+"> \""+graph_name+"\" .")
            else:
                print("<"+md5triple(str(s),str(p),str(o))+"> <"+context+"> \""+graph_name+"\" .")
    end = time()
    logger.info("RDF triples contextualization successfully completed in {}s".format(end - start))

    logger.info("RDF data from file '{}' successfully contextualized '{}'".format(rdf_file, graph_name))

if __name__ == '__main__':
    contextualize()
