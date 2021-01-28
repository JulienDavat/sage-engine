# setup.py
# Author: Thomas MINIER - MIT License 2017-2018
import setuptools

__version__ = "2.0.1"

# dependencies required for the HDT backend
HDT_DEPS = [
    'pybind11==2.2.4',
    'hdt==2.3'
]

# dependencies required for the PostgreSQL backend
POSTGRESQL_DEPS = [
    'psycopg2-binary==2.8.6'
]

console_scripts = [
    'sage = sage.cli.http_server:start_sage_server',
    'sage-query = sage.cli.commons:sage_query',
    'sage-client = sage.cli.commons:sage_client',
    'sage-explain = sage.cli.explain:explain',
    'sage-postgres-init = sage.cli.postgres:init_postgres',
    'sage-postgres-index = sage.cli.postgres:index_postgres',
    'sage-postgres-put = sage.cli.postgres:put_postgres',
    'sage-postgres-sput = sage.cli.postgres:stream_postgres'
]

with open('requirements.txt') as file:
    install_requires = file.read().splitlines()

setuptools.setup(
    name="sage-engine",
    version=__version__,
    author="Thomas Minier",
    author_email="thomas.minier@univ-nantes.fr",
    url="https://github.com/sage-org/sage-engine",
    description="Sage: a SPARQL query engine for public Linked Data providers",
    keywords=["rdf", "sparql", "query engine"],
    license="MIT",
    install_requires=install_requires,
    include_package_data=True,
    zip_safe=False,
    packages=setuptools.find_packages(exclude=["tests", "tests.*"]),
    # extras dependencies for the native backends
    extras_require={
        'hdt': HDT_DEPS,
        'postgres': POSTGRESQL_DEPS
    },
    entry_points={
        'console_scripts': console_scripts
    }
)
