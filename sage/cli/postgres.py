# postgres.py
# Author: Thomas MINIER - MIT License 2017-2019
import sage.cli.postgres_utils as p_utils
from sage.cli.utils import load_graph, get_rdf_reader
from sage.database.postgres.utils import predicate_to_id
import click
import psycopg2
from psycopg2.extras import execute_values
import coloredlogs
import logging
from time import time


def bucketify(iterable, bucket_size):
    """Group items from an iterable by buckets"""
    bucket = list()
    for s, p, o in iterable:
        # try to encode the predicate (if it is a general predicate)
        # p = predicate_to_id(p)
        bucket.append((s, p, o))
        if len(bucket) >= bucket_size:
            yield bucket
            bucket = list()
    if len(bucket) > 0:
        yield bucket


def connect_postgres(graph):
    """Try to connect to a PostgreSQL server"""
    if 'dbname' not in graph or 'user' not in graph or 'password' not in graph:
        print("Error: a valid PostgreSQL graph must be declared with fields 'dbname', 'user' and 'password'")
        return None
    dbname = graph['dbname']
    user = graph['user']
    password = graph['password']
    host = graph['host'] if 'host' in graph else ''
    port = int(graph['port']) if 'port' in graph else 5432
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


@click.command()
@click.argument("config")
@click.argument("graph_name")
@click.option('--index/--no-index', default=True, help="Enable/disable indexing of SQL tables. The indexes can be created separately using the command sage-postgre-index")
def init_postgres(config, graph_name, index):
    """
        Initialize the RDF graph GRAPH_NAME with a PostgreSQL/PostgreSQL-MVCC backend, described in the configuration file CONFIG.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # load graph from config file
    graph, kind = load_graph(config, graph_name, logger, backends=['postgres', 'postgres-mvcc'])
    enable_mvcc = kind == 'postgres-mvcc'

    # init postgre connection
    connection = connect_postgres(graph)
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False

    # create all SQL queries used to init the graph, using the graph name
    table_name = graph['name']
    create_table_query = p_utils.get_postgres_create_table(table_name, enable_mvcc=enable_mvcc)
    create_indexes_queries = p_utils.get_postgres_create_indexes(table_name, enable_mvcc=enable_mvcc)

    cursor = connection.cursor()
    # create the main SQL table
    logger.info("Creating SQL table {}...".format(table_name))
    cursor.execute(create_table_query)
    logger.info("SPARQL table {} successfully created".format(table_name))

    # create the additional inexes on OSP and POS
    if index:
        logger.info("Creating additional B-tree indexes...")
        for q in create_indexes_queries:
            cursor.execute(q)
        logger.info("Additional B-tree indexes successfully created")
    else:
        logger.info("Skipping additional indexes creation on user-demand")

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("Sage PostgreSQL model for table {} successfully initialized".format(table_name))


@click.command()
@click.argument("config")
@click.argument("graph_name")
def index_postgres(config, graph_name):
    """
        Create the additional B-tree indexes on the RDF graph GRAPH_NAME, described in the configuration file CONFIG. The graph must use the PostgreSQL or PostgreSQL-MVCC backend.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # load graph from config file
    graph, kind = load_graph(config, graph_name, logger, backends=['postgres', 'postgres-mvcc'])
    enable_mvcc = kind == 'postgres-mvcc'

    # init PostgreSQL connection
    connection = connect_postgres(graph)
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False
    # create all SQL queries used to init the graph, using the graph name
    table_name = graph['name']
    create_indexes_queries = p_utils.get_postgres_create_indexes(table_name, enable_mvcc=enable_mvcc)

    # create indexes
    cursor = connection.cursor()
    start = time()
    logger.info("Creating additional B-tree indexes...")
    for q in create_indexes_queries:
        cursor.execute(q)
    stop = time()
    logger.info("Additional B-tree indexes successfully created in {}s".format(stop - start))

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("Sage PostgreSQL model for table {} successfully initialized".format(table_name))


@click.command()
@click.argument("rdf_file")
@click.argument("config")
@click.argument("graph_name")
@click.option("-f", "--format", type=click.Choice(["nt", "ttl", "hdt","n3"]),
              default="nt", show_default=True, help="Format of the input file. Supported: nt (N-triples), ttl (Turtle) and hdt (HDT).")
@click.option("-b", "--block_size", type=int, default=100, show_default=True,
              help="Block size used for the bulk loading")
@click.option("-c", "--commit_threshold", type=int, default=500000, show_default=True,
              help="Commit after sending this number of RDF triples")
def put_postgres(config, graph_name, rdf_file, format, block_size, commit_threshold):
    """
        Insert RDF triples from file RDF_FILE into the RDF graph GRAPH_NAME, described in the configuration file CONFIG. The graph must use the PostgreSQL or PostgreSQL-MVCC backend.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # load graph from config file
    graph, kind = load_graph(config, graph_name, logger, backends=['postgres', 'postgres-mvcc'])
    enable_mvcc = kind == 'postgres-mvcc'

    # init PostgreSQL connection
    logger.info("Connecting to PostgreSQL server...")
    connection = connect_postgres(graph)
    logger.info("Connected to PostgreSQL server")
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False

    # compute SQL table name and the bulk load SQL query
    table_name = graph['name']
    insert_into_query = p_utils.get_postgres_insert_into(table_name, enable_mvcc=enable_mvcc)

    logger.info("Reading RDF source file...")
    iterator, nb_triples = get_rdf_reader(rdf_file, format=format)
    logger.info("RDF source file loaded. Found ~{} RDF triples to ingest.".format(nb_triples))

    logger.info("Starting RDF triples ingestion...")
    cursor = connection.cursor()

    # insert rdf triples
    start = time()
    to_commit = 0
    # insert by bucket (and show a progress bar)
    with click.progressbar(length=nb_triples,
                           label="Inserting RDF triples".format(nb_triples)) as bar:
        for bucket in bucketify(iterator, block_size):
            to_commit += len(bucket)
            # bulk load the bucket of RDF triples, then update progress bar
            execute_values(cursor, insert_into_query, bucket, page_size=block_size)
            bar.update(len(bucket))
            # commit if above threshold
            if to_commit >= commit_threshold:
                # logger.info("Commit threshold reached. Committing all changes...")
                connection.commit()
                # logger.info("All changes were successfully committed.")
                to_commit = 0
    end = time()
    logger.info("RDF triples ingestion successfully completed in {}s".format(end - start))

    # run an ANALYZE query to rebuild statistics
    logger.info("Rebuilding table statistics...")
    start = time()
    cursor.execute("ANALYZE {}".format(table_name))
    end = time()
    logger.info("Table statistics successfully rebuilt in {}s".format(end - start))

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("RDF data from file '{}' successfully inserted into RDF graph '{}'".format(rdf_file, table_name))


#--- 8<-------------

from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink
import sys

class StreamSink(Sink):
    """
    A sink is used to store the results of parsing, this almost matches the sink
    example shown in ntriples:
      https://github.com/RDFLib/rdflib/blob/395a40101fe133d97f454ee61da0fc748a93b007/rdflib/plugins/parsers/ntriples.py#L43
    """
    bucket = list()
    length = 0
    to_commit=0

    def __init__(self, bucket_size,insert_into_query,cursor,connection,commit_threshold,logger):
        self._bucket_size = bucket_size
        self._insert_into_query=insert_into_query
        self._cursor=cursor
        self._connection=connection
        self._commit_threshold=commit_threshold
        # insert rdf triples
        self.to_commit = 0
        self._logger=logger

    def triple(self, s, p, o):
        self.length += 1

        # max size for a btree index cell in postgres
        if sys.getsizeof(o)<2730:
            # try str(o) to get rid of strange unicode...
            # a= rdflib.term.Literal('What tнe⃗ ♯$*! D⃗\ud835\udfb1 \ud835\udcccΣ (k)πow!?', lang='en')
            # encode -> transform string to byte using encoding, replace fix wrong unicode
            self.bucket.append((s, p, str(o).encode('utf-8','replace').decode('utf-8')))
        else:
            self._logger.info("truncated object {}".format(o))
            # 2 bytes for an utf-8 ??
            self.bucket.append((s,p,o[:1000]))
        self.to_commit +=1
        if len(self.bucket) >= self._bucket_size:
            try :
                execute_values(self._cursor, self._insert_into_query, self.bucket, page_size=self._bucket_size)
            except:
                print("Unexpected error:", sys.exc_info()[0])
                self._logger.error("bucket rejected {}".format(self.bucket))
            self.bucket = list()

        if self.to_commit >= self._commit_threshold:
            # logger.info("Commit threshold reached. Committing all changes...")
            self._connection.commit();
            self._logger.info("inserted {} triples".format(self.length))
            self.to_commit = 0

        #print "Stream of triples s={s}, p={p}, o={o}".format(s=s, p=p, o=o)


@click.command()
@click.argument("rdf_file")
@click.argument("config")
@click.argument("graph_name")
@click.option("-b", "--block_size", type=int, default=100, show_default=True,
              help="Block size used for the bulk loading")
@click.option("-c", "--commit_threshold", type=int, default=500000, show_default=True,
              help="Commit after sending this number of RDF triples")
def stream_postgres(config, graph_name, rdf_file, block_size, commit_threshold):
    """
        Insert RDF triples from file RDF_FILE into the RDF graph GRAPH_NAME, described in the configuration file CONFIG. The graph must use the PostgreSQL or PostgreSQL-MVCC backend.
    """
    # install logger
    coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
    logger = logging.getLogger(__name__)

    # load graph from config file
    graph, kind = load_graph(config, graph_name, logger, backends=['postgres', 'postgres-mvcc'])
    enable_mvcc = kind == 'postgres-mvcc'

    # init PostgreSQL connection
    logger.info("Connecting to PostgreSQL server...")
    connection = connect_postgres(graph)
    logger.info("Connected to PostgreSQL server")
    if connection is None:
        exit(1)
    # turn off autocommit
    connection.autocommit = False

    # compute SQL table name and the bulk load SQL query
    table_name = graph['name']
    insert_into_query = p_utils.get_postgres_insert_into(table_name, enable_mvcc=enable_mvcc)

    cursor = connection.cursor()


    logger.info("Reading NT RDF source file...(I hope)")
#    iterator, nb_triples = get_rdf_reader(rdf_file, format=format)

    start = time()
    n = NTriplesParser(StreamSink(block_size,insert_into_query,cursor,connection,commit_threshold,logger))
    with open(rdf_file, "rb") as anons:
        n.parse(anons)

    end = time()
    logger.info("RDF triples ingestion successfully completed in {}s".format(end - start))

    # run an ANALYZE query to rebuild statistics
    logger.info("Rebuilding table statistics...")
    start = time()
    cursor.execute("ANALYZE {}".format(table_name))
    end = time()
    logger.info("Table statistics successfully rebuilt in {}s".format(end - start))

    # commit and cleanup connection
    logger.info("Committing and cleaning up...")
    connection.commit()
    cursor.close()
    connection.close()
    logger.info("RDF data from file '{}' successfully inserted into RDF graph '{}'".format(rdf_file, table_name))
