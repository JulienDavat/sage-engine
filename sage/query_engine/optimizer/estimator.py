# join_builder.py
# Author: Thomas MINIER and Julien AIMONIER-DAVAT - MIT License 2017-2020
from datetime import datetime
from typing import Dict, Optional

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.scan import ScanIterator
from rdflib.paths import Path, SequencePath, AlternativePath, InvPath, NegatedPath, MulPath, OneOrMore, ZeroOrMore, ZeroOrOne
from rdflib import URIRef
import math

def estimate_sequence_path_cardinality(path_pattern: Dict[str, str], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> int:
    path = path_pattern['predicate']
    forwardCardinality =  estimate_cardinality({
        'subject': path_pattern['subject'],
        'predicate': path.args[0],
        'object': '?o',
        'graph': path_pattern['graph']
    }, dataset, default_graph, as_of=as_of)
    backwardCardinality = estimate_cardinality({
        'subject': '?s',
        'predicate': path.args[len(path.args) - 1],
        'object': path_pattern['object'],
        'graph': path_pattern['graph']
    }, dataset, default_graph, as_of=as_of)
    return forwardCardinality if forwardCardinality < backwardCardinality else backwardCardinality
    # cardinality = estimate_cardinality({
    #     'subject': path_pattern['subject'],
    #     'predicate': path.args[0],
    #     'object': '?o',
    #     'graph': path_pattern['graph']
    # }, dataset, default_graph, as_of=as_of)
    # for i in range(1, len(path.args) - 1):
    #     card = estimate_cardinality({
    #         'subject': '?s',
    #         'predicate': path.args[i],
    #         'object': '?o',
    #     'graph': path_pattern['graph']
    #     }, dataset, default_graph, as_of=as_of)
    #     cardinality = card if card > cardinality else cardinality
    # card = estimate_cardinality({
    #     'subject': '?s',
    #     'predicate': path.args[len(path.args) - 1],
    #     'object': path_pattern['object'],
    #     'graph': path_pattern['graph']
    # }, dataset, default_graph, as_of=as_of)
    # return card if card > cardinality else cardinality


def estimate_alternative_path_cardinality(path_pattern: Dict[str, str], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> int:
    path = path_pattern['predicate']
    cardinality = estimate_cardinality({
        'subject': path_pattern['subject'],
        'predicate': path.args[0],
        'object': path_pattern['object'],
        'graph': path_pattern['graph']
    }, dataset, default_graph, as_of=as_of)
    for i in range(1, len(path.args) - 1):
        cardinality += estimate_cardinality({
            'subject': path_pattern['subject'],
            'predicate': path.args[i],
            'object': path_pattern['object'],
        'graph': path_pattern['graph']
        }, dataset, default_graph, as_of=as_of)
    cardinality += estimate_cardinality({
        'subject': path_pattern['subject'],
        'predicate': path.args[len(path.args) - 1],
        'object': path_pattern['object'],
        'graph': path_pattern['graph']
    }, dataset, default_graph, as_of=as_of)
    return cardinality


def estimate_inverse_path_cardinality(path_pattern: Dict[str, str], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> int:
    path = path_pattern['predicate']
    return estimate_cardinality({
        'subject': path_pattern['object'],
        'predicate': path.arg,
        'object': path_pattern['subject'],
        'graph': path_pattern['graph']
    }, dataset, default_graph, as_of=as_of)


def estimate_negated_path_cardinality(path_pattern: Dict[str, str], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> int:
    return estimate_cardinality({
        'subject': path_pattern['subject'],
        'predicate': '?p',
        'object': path_pattern['object'],
        'graph': path_pattern['graph']
    }, dataset, default_graph, as_of=as_of)


def compute_closure_estimation(relation_size: int, starter_cardinality: int, max_depth: int) -> int:
    cardinality = starter_cardinality
    branching_factor = starter_cardinality
    if starter_cardinality == 0:
        return 0
    for i in range(max_depth):
        if (branching_factor / 3) >= 1:
            branching_factor = branching_factor / 3
        else:
            branching_factor = 1
        cardinality = cardinality * branching_factor
        if cardinality > relation_size:
            return relation_size
    return cardinality


def estimate_closure_path_cardinality(path_pattern: Dict[str, str], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> int:
    path = path_pattern['predicate']
    
    if not path_pattern['subject'].startswith('?'):
        return estimate_cardinality({
            'subject': path_pattern['subject'],
            'predicate': path.path,
            'object': '?o',
            'graph': path_pattern['graph']
        }, dataset, default_graph, as_of=as_of)
    elif not path_pattern['object'].startswith('?'):
        return estimate_cardinality({
            'subject': '?s',
            'predicate': path.path,
            'object': path_pattern['object'],
            'graph': path_pattern['graph']
        }, dataset, default_graph, as_of=as_of)
    else:
        return estimate_cardinality({
            'subject': '?s',
            'predicate': path.path,
            'object': '?o',
            'graph': path_pattern['graph']
        }, dataset, default_graph, as_of=as_of)

    # max_depth = dataset.get_graph(default_graph).max_depth
    # relation_size = estimate_cardinality({
    #     'subject': '?s',
    #     'predicate': path.path,
    #     'object': '?o',
    #     'graph': path_pattern['graph']
    # }, dataset, default_graph, as_of=as_of)
    # if not path_pattern['subject'].startswith('?'):
    #     starter_cardinality = estimate_cardinality({
    #         'subject': path_pattern['subject'],
    #         'predicate': path.path,
    #         'object': '?o',
    #         'graph': path_pattern['graph']
    #     }, dataset, default_graph, as_of=as_of)
    #     print(starter_cardinality)
    #     cardinality = compute_closure_estimation(relation_size, starter_cardinality, max_depth)
    #     print(cardinality)
    #     return cardinality
    # elif not path_pattern['object'].startswith('?'):
    #     starter_cardinality = estimate_cardinality({
    #         'subject': '?s',
    #         'predicate': path.path,
    #         'object': path_pattern['object'],
    #         'graph': path_pattern['graph']
    #     }, dataset, default_graph, as_of=as_of)
    #     print(starter_cardinality)
    #     cardinality = compute_closure_estimation(relation_size, starter_cardinality, max_depth)
    #     print(cardinality)
    #     return cardinality
    # else:
    #     return relation_size
    
    # if not path_pattern['subject'].startswith('?') or not path_pattern['object'].startswith('?'):
    #     return 1
    # else:
        # return estimate_cardinality({
        #     'subject': '?s',
        #     'predicate': path.path,
        #     'object': '?o',
        #     'graph': path_pattern['graph']
        # }, dataset, default_graph, as_of=as_of) #* max_depth


def estimate_path_cardinality(path_pattern: Dict[str, str], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> int:
    path = path_pattern['predicate']
    if type(path) is MulPath:
        return estimate_closure_path_cardinality(path_pattern, dataset, default_graph, as_of=as_of)
    elif type(path) is NegatedPath:
        return estimate_negated_path_cardinality(path_pattern, dataset, default_graph, as_of=as_of)
    elif type(path) is SequencePath:
        return estimate_sequence_path_cardinality(path_pattern, dataset, default_graph, as_of=as_of)
    elif type(path) is AlternativePath:
        return estimate_alternative_path_cardinality(path_pattern, dataset, default_graph, as_of=as_of)
    elif type(path) is InvPath:
        return estimate_inverse_path_cardinality(path_pattern, dataset, default_graph, as_of=as_of)
    elif type(path) is URIRef:
        return ScanIterator(path_pattern, dataset, as_of=as_of).__len__()
    else:
        raise Exception(f'Path: unexpected path type: {type(path)}')


def estimate_cardinality(triple: Dict[str, str], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> int:
    if isinstance(triple['predicate'], Path):
        return estimate_path_cardinality(triple, dataset, default_graph, as_of=as_of)
    else:
        return ScanIterator(triple, dataset, as_of=as_of).__len__()


def compute_cardinality(triple: Dict[str, str], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> int:
    cardinality = estimate_cardinality(triple, dataset, default_graph, as_of=as_of)
    # print(triple)
    # print(cardinality)
    if cardinality == 0:
        return 0
    elif isinstance(triple['predicate'], Path):
        cardinality = math.floor(math.log(cardinality, 10)) + 1.5
    else:
        cardinality = math.floor(math.log(cardinality, 10))
    # print(cardinality)
    return cardinality