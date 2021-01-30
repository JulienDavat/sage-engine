# join_builder.py
# Author: Thomas MINIER and Julien AIMONIER-DAVAT - MIT License 2017-2020
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.bind import BindIterator
from sage.query_engine.iterators.ppaths.piggyback import PiggyBackIterator
from sage.query_engine.iterators.ppaths.control_tuples_memory import ControlTuplesBuffer
from sage.query_engine.iterators.ppaths.v1.simple_depth_annotation_memory import TransitiveClosureIterator
from sage.query_engine.iterators.ppaths.reflexive_closure import ReflexiveClosureIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.utils import EmptyIterator
from sage.query_engine.optimizer.utils import equality_variables, find_connected_pattern, get_vars
from rdflib.paths import Path, SequencePath, AlternativePath, InvPath, NegatedPath, MulPath, OneOrMore, ZeroOrMore, ZeroOrOne
from rdflib import URIRef
import sys, time
from sage.query_engine.optimizer.estimator import estimate_cardinality


def create_equality_expr(variable, values):
    if len(values) == 1:
        return f'({variable} != <{values.pop()}>)'
    else:
        expr = f'({variable} != <{values.pop()}>)'
        return f'({expr} && {create_equality_expr(variable, values)})'
    

def parse_sequence_path(path_pattern: Dict[str, str], forward: bool, dataset: Dataset, default_graph: str, control_tuples: ControlTuplesBuffer, as_of: Optional[datetime] = None) -> PreemptableIterator:
    path = path_pattern['predicate']
    unique_prefix = time.time_ns()
    iterators = []
    index = 0
    iterator = parse_path_pattern({
        'subject': path_pattern['subject'], 
        'predicate': path_pattern['predicate'].args[index],
        'object': f'?seq_{unique_prefix}_{index}',
        'graph': path_pattern['graph']
    }, forward, dataset, default_graph, control_tuples, as_of=as_of)
    iterators.append(iterator)
    while index < len(path.args) - 2:
        iterator = parse_path_pattern({
            'subject': f'?seq_{unique_prefix}_{index}',
            'predicate': path.args[index + 1],
            'object': f'?seq_{unique_prefix}_{index + 1}',
            'graph': path_pattern['graph']
        }, forward, dataset, default_graph, control_tuples, as_of=as_of)
        iterators.append(iterator)
        index = index + 1
    iterator = parse_path_pattern({
        'subject': f'?seq_{unique_prefix}_{index}',
        'predicate': path.args[index + 1],
        'object': path_pattern['object'],
        'graph': path_pattern['graph']
    }, forward, dataset, default_graph, control_tuples, as_of=as_of)
    iterators.append(iterator)
    if not forward:
        iterators.reverse()
    pipeline = iterators.pop(0)
    while len(iterators) > 0:
        pipeline = IndexJoinIterator(pipeline, iterators.pop(0))
    return pipeline


def parse_alternative_path(path_pattern: Dict[str, str], forward: bool, dataset: Dataset, default_graph: str, control_tuples: ControlTuplesBuffer, as_of: Optional[datetime] = None) -> PreemptableIterator:
    path = path_pattern['predicate']
    pipeline = parse_path_pattern({
        'subject': path_pattern['subject'],
        'predicate': path.args[0],
        'object': path_pattern['object'],
        'graph': path_pattern['graph']
    }, forward, dataset, default_graph, control_tuples, as_of=as_of)
    for index in range(1, len(path.args)):
        iterator = parse_path_pattern({
            'subject': path_pattern['subject'],
            'predicate': path.args[index],
            'object': path_pattern['object'],
            'graph': path_pattern['graph']
        }, forward, dataset, default_graph, control_tuples, as_of=as_of)
        pipeline = BagUnionIterator(pipeline, iterator)
    return pipeline


def parse_inverse_path(path_pattern: Dict[str, str], forward: bool, dataset: Dataset, default_graph: str, control_tuples: ControlTuplesBuffer, as_of: Optional[datetime] = None) -> PreemptableIterator:
    path = path_pattern['predicate']
    return parse_path_pattern({
            'subject': path_pattern['object'], 
            'predicate': path.arg, 
            'object': path_pattern['subject'],
            'graph': path_pattern['graph']
        }, forward, dataset, default_graph, control_tuples, as_of=as_of)


def parse_negated_property_set_expression(path_pattern: Dict[str, str], dataset: Dataset, default_graph: str, control_tuples: ControlTuplesBuffer, as_of: Optional[datetime] = None) -> PreemptableIterator:
    path = path_pattern['predicate']
    if type(path) is NegatedPath:
        unique_prefix = time.time_ns()
        forward_properties = []
        for arg in path.args:
            if type(arg) is URIRef:
                forward_properties.append(str(arg))
            else:
                raise Exception('PropertyPaths: Negated property sets with reverse properties are not supported yet...')
        scan = ScanIterator({
            'subject': path_pattern['subject'],
            'predicate': f'?neg_{unique_prefix}_{1}',
            'object': path_pattern['object'],
            'graph': path_pattern['graph']
        }, dataset, as_of=as_of)
        return FilterIterator(scan, create_equality_expr(f'?neg_{unique_prefix}_{1}', forward_properties))
    else:
        raise Exception(f'PropertyPaths: {type(path)} is not a negated property set expression !')


def parse_closure_expression(path_pattern: Dict[str, str], forward: bool, dataset: Dataset, default_graph: str, control_tuples: ControlTuplesBuffer, as_of: Optional[datetime] = None) -> PreemptableIterator:
    path = path_pattern['predicate']
    print(path)
    star_id = time.time_ns()
    min_depth = 1 if path.mod == OneOrMore else 0
    max_depth = 1 if path.mod == ZeroOrOne else dataset.get_graph(default_graph).max_depth
    iterators = []

    # Uncomment to use the v2 of the PTC operators (iterators on the stack are suspended)

    # if forward:
    #     iterator = parse_path_pattern({
    #         'subject': '?source',
    #         'predicate': path.path,
    #         'object': '?node',
    #         'graph': path_pattern['graph']
    #     }, forward, dataset, default_graph, control_tuples, as_of)
    #     transitive_closure = TransitiveClosureIterator(star_id, path_pattern['subject'], str(path_pattern['predicate']), path_pattern['object'], iterator, forward, dataset, min_depth=min_depth, max_depth=max_depth)
    # else:
    #     iterator = parse_path_pattern({
    #         'subject': '?node',
    #         'predicate': path.path,
    #         'object': '?source',
    #         'graph': path_pattern['graph']
    #     }, forward, dataset, default_graph, control_tuples, as_of)
    #     transitive_closure = TransitiveClosureIterator(star_id, path_pattern['object'], str(path_pattern['predicate']), path_pattern['subject'], iterator, forward, dataset, min_depth=min_depth, max_depth=max_depth)
    
    # Uncomment to use the v1 of the PTC operators (iterators on the stack are not suspended)

    if forward:
        iterator = parse_path_pattern({
            'subject': path_pattern['subject'],
            'predicate': path.path,
            'object': f'?star_{star_id}_{0}',
            'graph': path_pattern['graph']
        }, forward, dataset, default_graph, control_tuples, as_of)
        iterators.append(iterator)
        for depth in range(1, max_depth + 1):
            iterator = parse_path_pattern({
                'subject': f'?star_{star_id}_{depth - 1}',
                'predicate': path.path,
                'object': f'?star_{star_id}_{depth}',
                'graph': path_pattern['graph']
            }, forward, dataset, default_graph, control_tuples, as_of)
            iterators.append(iterator)
        transitive_closure = TransitiveClosureIterator(path_pattern['subject'], str(path_pattern['predicate']), path_pattern['object'], forward, iterators, id=star_id, min_depth=min_depth, max_depth=max_depth)
    else:
        iterator = parse_path_pattern({
            'subject': f'?star_{star_id}_{0}',
            'predicate': path.path,
            'object': path_pattern['object'],
            'graph': path_pattern['graph']
        }, forward, dataset, default_graph, control_tuples, as_of)
        iterators.append(iterator)
        for depth in range(1, max_depth + 1):
            iterator = parse_path_pattern({
                'subject': f'?star_{star_id}_{depth}',
                'predicate': path.path,
                'object': f'?star_{star_id}_{depth - 1}',
                'graph': path_pattern['graph']
            }, forward, dataset, default_graph, control_tuples, as_of)
            iterators.append(iterator)
        transitive_closure = TransitiveClosureIterator(path_pattern['object'], str(path_pattern['predicate']), path_pattern['subject'], forward, iterators, id=star_id, min_depth=min_depth, max_depth=max_depth)
    
    transitive_closure = PiggyBackIterator(transitive_closure, control_tuples)

    if min_depth == 0:
        spo_pattern = {'subject': '?s', 'predicate': '?p', 'object': '?o', 'graph': path_pattern['graph']}
        spo_scan = ScanIterator(spo_pattern, dataset, as_of=as_of)
        reflexive_closure = ReflexiveClosureIterator(path_pattern['subject'], path_pattern['object'], spo_scan)
        return BagUnionIterator(transitive_closure, reflexive_closure)
    else:
        return transitive_closure


def parse_path_pattern(path_pattern: Dict[str, str], forward: bool, dataset: Dataset, default_graph: str, control_tuples: ControlTuplesBuffer, as_of: Optional[datetime] = None) -> PreemptableIterator:
    path = path_pattern['predicate']
    if type(path) is MulPath:
        return parse_closure_expression(path_pattern, forward, dataset, default_graph, control_tuples, as_of=as_of)
    elif type(path) is NegatedPath:
        return parse_negated_property_set_expression(path_pattern, dataset, default_graph, control_tuples, as_of=as_of)
    elif type(path) is SequencePath:
        return parse_sequence_path(path_pattern, forward, dataset, default_graph, control_tuples, as_of=as_of)
    elif type(path) is AlternativePath:
        return parse_alternative_path(path_pattern, forward, dataset, default_graph, control_tuples, as_of=as_of)
    elif type(path) is InvPath:
        return parse_inverse_path(path_pattern, forward, dataset, default_graph, control_tuples, as_of=as_of)
    elif type(path) is URIRef:
        return ScanIterator(path_pattern, dataset, as_of=as_of)
    else:
        raise Exception(f'Path: unexpected path type: {type(path)}')


def parse_pattern(pattern: Dict[str, str], query_vars: Set[str], dataset: Dataset, default_graph: str, control_tuples: ControlTuplesBuffer, as_of: Optional[datetime] = None) -> PreemptableIterator:
    if pattern['cardinality'] == 0:
        return EmptyIterator()
    if isinstance(pattern['triple']['predicate'], Path):
        if pattern['triple']['subject'] in query_vars:
            forward = True
        elif pattern['triple']['object'] in query_vars:
            forward = False
        elif not pattern['triple']['subject'].startswith('?'):
            forward = True
        elif not pattern['triple']['object'].startswith('?'):
            forward = False
        else:
            forward = True
        return parse_path_pattern(pattern['triple'], forward, dataset, default_graph, control_tuples, as_of=as_of)
    else:
        return ScanIterator(pattern['triple'], dataset, as_of=as_of)


def build_left_join_tree(bgp: List[Dict[str, str]], query_vars: Set[str], dataset: Dataset, default_graph: str, control_tuples: ControlTuplesBuffer, as_of: Optional[datetime] = None) -> Tuple[PreemptableIterator, List[Dict[str, str]], Set[str]]:
    # gather metadata about triple patterns
    cardinalities = []

    # not common but happen in query insert where { bind }
    if len(bgp)==0:
        return EmptyIterator(),[],set()

    for triple in bgp:
        # select the graph used to evaluate the pattern
        graph_uri = triple['graph'] if 'graph' in triple and len(triple['graph']) > 0 else default_graph
        triple['graph'] = graph_uri
        if dataset.has_graph(graph_uri):
            selectivity = estimate_cardinality(triple, dataset, default_graph, as_of=as_of)
        else:
            selectivity = 0
        cardinalities += [{'triple': triple, 'cardinality': selectivity}]

    # sort triples by ascending selectivity
    triples = sorted(cardinalities, key=lambda v: v['cardinality'])

    # build the left linear tree of joins
    pattern, pos, _ = find_connected_pattern(query_vars, triples)
    if pattern is None:
        pattern = triples[0]
        pos = 0        

    pipeline = parse_pattern(pattern, query_vars, dataset, default_graph, control_tuples, as_of=as_of)
    query_vars = query_vars | get_vars(pattern['triple'])
    triples.pop(pos)
    while len(triples) > 0:
        pattern, pos, _ = find_connected_pattern(query_vars, triples)
        # no connected pattern = disconnected BGP => pick the first remaining pattern in the BGP
        if pattern is None:
            pattern = triples[0]
            pos = 0
        iterator = parse_pattern(pattern, query_vars, dataset, default_graph, control_tuples, as_of=as_of)
        query_vars = query_vars | get_vars(pattern['triple'])
        pipeline = IndexJoinIterator(pipeline, iterator)
        triples.pop(pos)

    return pipeline, cardinalities, query_vars