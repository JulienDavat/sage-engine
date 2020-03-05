# bgp_interface_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from sage.http_server.server import run_app
from starlette.testclient import TestClient
from tests.http.utils import post_sparql

filter_queries = [
    ("""
        select ?s where {
          ?s <http://isa> ?o
        }
    """, 5),
    ("""
         select ?o1 where {
          ?s <http://isa> ?o
          BIND(<http://example.org/rowid>(?s,<http://isa>,?o) as ?z)
          ?z <http://source> ?o1 }
    """, 5)
]


class TestRowIdInterface(object):
    @classmethod
    def setup_class(self):
        self._app = run_app('tests/data/test_config.yaml')
        self._client = TestClient(self._app)

    @classmethod
    def teardown_class(self):
        pass

    @pytest.mark.parametrize('query,cardinality', filter_queries)
    def test_filter_interface(self, query, cardinality):
        nbResults = 0
        nbCalls = 0
        hasNext = True
        next_link = None
        while hasNext:
            response = post_sparql(self._client, query, next_link, 'http://localhost:8000/sparql/context')
            assert response.status_code == 200
            response = response.json()
            nbResults += len(response['bindings'])
            hasNext = response['hasNext']
            next_link = response['next']
            nbCalls += 1
            print(response)
        assert nbResults == cardinality
        assert nbCalls >= 1
