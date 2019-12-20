# grpc_server.py
# Author: Thomas MINIER - MIT License 2017-2020
import logging
from asyncio import run, set_event_loop_policy
from concurrent.futures import ThreadPoolExecutor
from time import time

import grpc
import uvloop
from sage.database.core.yaml_config import load_config
from sage.grpc import service_pb2_grpc
from sage.grpc.service_pb2 import Binding, BindingSet, SageResponse
from sage.http_server.utils import decode_saved_plan, encode_saved_plan
from sage.query_engine.iterators.loader import load
from sage.query_engine.optimizer.query_parser import parse_query
from sage.query_engine.sage_engine import SageEngine


def create_bindings(bindings):
  """Create an iterator that converts a set of dict-based bindings to a set of protobuf-based bindings"""
  for binding in bindings:
    binding_set = BindingSet()
    for variable, value in binding.items():
      binding_set.values.append(Binding(variable = variable, value = value))
    yield binding_set


class SageQueryService(service_pb2_grpc.SageSPARQLServicer):

  def __init__(self, dataset):
    super().__init__()
    self._dataset = dataset
    self._engine = SageEngine()
  
  def Query(self, request, context):
    query = request.query
    graph_name = request.default_graph_uri
    next_link = request.next_link if len(request.next_link) > 0 else None
    if not self._dataset.has_graph(graph_name):
      raise SystemError(f"RDF Graph {graph_name} not found on the server.")
    graph = self._dataset.get_graph(graph_name)

    # decode next_link or build query execution plan
    cardinalities = dict()
    start = time()
    if next_link is not None:
      if self._dataset.is_stateless:
          saved_plan = next_link
      else:
          saved_plan = self._dataset.statefull_manager.get_plan(next_link)
      plan = load(decode_saved_plan(saved_plan), self._dataset)
    else:
      plan, cardinalities = parse_query(query, self._dataset, graph_name, '')
    loading_time = (time() - start) * 1000

    # execute query
    engine = SageEngine()
    quota = graph.quota / 1000
    max_results = graph.max_results
    bindings, saved_plan, is_done, abort_reason = run(engine.execute(plan, quota, max_results))

    # commit or abort (if necessary)
    if abort_reason is not None:
      graph.abort()
      raise SystemError(f"The SPARQL query has been aborted for the following reason: '{abort_reason}'")
    else:
      graph.commit()

    # encode saved plan if query execution is not done yet and there was no abort
    start = time()
    next_page = None
    if (not is_done) and abort_reason is None:
      next_page = encode_saved_plan(saved_plan)
      if not self._dataset.is_stateless:
        # generate the plan ID if this is the first time we execute this plan
        plan_id = next_link if next_link is not None else str(uuid4())
        self._dataset.statefull_manager.save_plan(plan_id, next_page)
        next_page = plan_id
    elif is_done and (not self._dataset.is_stateless) and next_link is not None:
      # delete the saved plan, as it will not be reloaded anymore
      self._dataset.statefull_manager.delete_plan(next_link)
    exportTime = (time() - start) * 1000

    # create response
    response = SageResponse(is_done = is_done, next_link = next_page)
    for binding in create_bindings(bindings):
      response.bindings.append(binding)
    return response


def get_server(config_file, port='8000', workers=10):
  """Create a SaGe SPARQL query server powered by GRPC"""
  set_event_loop_policy(uvloop.EventLoopPolicy())
  logging.basicConfig()

  dataset = load_config(config_file)
  service = SageQueryService(dataset)

  server = grpc.server(ThreadPoolExecutor(max_workers=10))
  service_pb2_grpc.add_SageSPARQLServicer_to_server(service, server)

  server.add_insecure_port(f'[::]:{port}')
  return server
