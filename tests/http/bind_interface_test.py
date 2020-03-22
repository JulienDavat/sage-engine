# bgp_interface_test.py
# Author: Thomas MINIER - MIT License 2017-2018
import pytest
from sage.http_server.server import run_app
from starlette.testclient import TestClient
from tests.http.utils import post_sparql

filter_queries = [
    ("""
        select ?md5 where {
          ?s <http://isa> ?o
          BIND(URI(CONCAT("http://",MD5(CONCAT(STR(?s),STR(<http://isa>),STR(?o))))) as ?md5)
        }
    """, 5),
    ("""
         select ?o1 where {
          ?s <http://isa> ?o
          BIND(URI(CONCAT("http://",MD5(CONCAT(STR(?s),STR(<http://isa>),STR(?o))))) as ?md5)
          ?md5 <http://source> ?o1 }
    """, 5),
    ("""
        select ?md5 where {
        BIND(URI(CONCAT("http://",MD5(CONCAT(STR(<http://donald>),STR(<http://isa>),STR("jerk"))))) as ?md5)
        }
    """, 1)
]


class TestBindInterface(object):
    @classmethod
    def setup_class(self):
        self._app = run_app('tests/data/test_config.yaml')
        self._client = TestClient(self._app)

    @classmethod
    def teardown_class(self):
        pass

    @pytest.mark.parametrize('query,cardinality', filter_queries)
    def test_bind_interface(self, query, cardinality):
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
            #print(response)
        assert nbResults == cardinality
        assert nbCalls >= 1
