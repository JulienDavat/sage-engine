# join_builder.py
# Author: Thomas MINIER and Julien AIMONIER-DAVAT - MIT License 2017-2020
from datetime import datetime
from typing import Dict, Optional

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.scan import ScanIterator
from rdflib.paths import Path, SequencePath, AlternativePath, InvPath, NegatedPath, MulPath, OneOrMore, ZeroOrMore, ZeroOrOne
from rdflib import URIRef


def estimate_sequence_path_cardinality(path_pattern: Dict[str, str], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> int:
    path = path_pattern['predicate']
    cardinality = estimate_cardinality({
        'subject': path_pattern['subject'],
        'predicate': path.args[0],
        'object': '?o',
        'graph': path_pattern['graph']
    }, dataset, default_graph, as_of=as_of)
    for i in range(1, len(path.args) - 1):
        card = estimate_cardinality({
            'subject': '?s',
            'predicate': path.args[i],
            'object': '?o',
        'graph': path_pattern['graph']
        }, dataset, default_graph, as_of=as_of)
        cardinality = card if card > cardinality else cardinality
    card = estimate_cardinality({
        'subject': '?s',
        'predicate': path.args[len(path.args) - 1],
        'object': path_pattern['object'],
        'graph': path_pattern['graph']
    }, dataset, default_graph, as_of=as_of)
    return card if card > cardinality else cardinality


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


def estimate_closure_path_cardinality(path_pattern: Dict[str, str], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> int:
    path = path_pattern['predicate']
    max_depth = dataset.get_graph(default_graph).max_depth
    if not path_pattern['subject'].startswith('?') or not path_pattern['object'].startswith('?'):
        return 1
    else:
        return estimate_cardinality({
            'subject': '?s',
            'predicate': path.path,
            'object': '?o',
            'graph': path_pattern['graph']
        }, dataset, default_graph, as_of=as_of) * max_depth


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
