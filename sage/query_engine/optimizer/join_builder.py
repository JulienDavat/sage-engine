# join_builder.py
# Author: Thomas MINIER - MIT License 2017-2020
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.transitive_closure import TransitiveClosureIterator
from sage.query_engine.iterators.reflexive_closure import ReflexiveClosureIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.utils import EmptyIterator
from sage.query_engine.optimizer.utils import (equality_variables,
                                               find_connected_pattern,
                                               get_vars)
from rdflib.paths import Path, SequencePath, AlternativePath, InvPath, NegatedPath, MulPath, OneOrMore, ZeroOrMore, ZeroOrOne
from rdflib import URIRef
import sys

closure_max_depth = 15

def create_equality_expr(variable, values):
    if len(values) == 1:
        return f'({variable} != <{values.pop()}>)'
    else:
        expr = f'({variable} != <{values.pop()}>)'
        return f'({expr} && {create_equality_expr(variable, values)})'

def parse_property_path(subject: str, path: Path, obj: str, graph: str, dataset: Dataset, forward: bool = True, inverse: bool = False, as_of: Optional[datetime] = None) -> PreemptableIterator:
    var_prefix = f'{subject[1:]}_' if subject.startswith('?') else ( f'{obj[1:]}_' if obj.startswith('?') else 'var_' )
    if type(path) is SequencePath:
        print('sequence path')
        print(path.args)
        if forward:
            index = 0
            arg = path.args[index]
            pipeline = parse_property_path(subject, arg, f'?{var_prefix}{index}', graph, dataset, forward, inverse, as_of)
            while index < len(path.args) - 2:
                arg = path.args[index + 1]
                right = parse_property_path(f'?{var_prefix}{index}', arg, f'?{var_prefix}{index + 1}', graph, dataset, forward, inverse, as_of)
                pipeline = IndexJoinIterator(pipeline, right)
                index += 1
            arg = path.args[index + 1]
            right = parse_property_path(f'?{var_prefix}{index}', arg, obj, graph, dataset, forward, inverse, as_of)
            pipeline = IndexJoinIterator(pipeline, right)
        else:
            index = len(path.args) - 1
            arg = path.args[index]
            pipeline = parse_property_path(subject, arg, f'?{var_prefix}{index}', graph, dataset, forward, inverse, as_of)
            while index > 1:
                arg = path.args[index - 1]
                right = parse_property_path(f'?{var_prefix}{index}', arg, f'?{var_prefix}{index - 1}', graph, dataset, forward, inverse, as_of)
                pipeline = IndexJoinIterator(pipeline, right)
                index -= 1
            arg = path.args[index - 1]
            right = parse_property_path(f'?{var_prefix}{index}', arg, obj, graph, dataset, forward, inverse, as_of)
            pipeline = IndexJoinIterator(pipeline, right)
        return pipeline
    elif type(path) is AlternativePath:
        print('alternative path')
        print(path.args)
        index = 0
        arg = path.args[index]
        pipeline = parse_property_path(subject, arg, obj, graph, dataset, forward, inverse, as_of)
        while index < len(path.args) - 2:
            arg = path.args[index + 1]
            right = parse_property_path(subject, arg, obj, graph, dataset, forward, inverse, as_of)
            pipeline = BagUnionIterator(pipeline, right)
            index += 1
        arg = path.args[index + 1]
        right = parse_property_path(subject, arg, obj, graph, dataset, forward, inverse, as_of)
        pipeline = BagUnionIterator(pipeline, right)
        return pipeline
    elif type(path) is InvPath:
        print('inverse path')
        print(path.arg)
        return parse_property_path(subject, path.arg, obj, graph, dataset, forward, not inverse, as_of)
    elif type(path) is MulPath:
        print('repeat path')
        print(path.mod)
        print(path.path)
        global closure_max_depth
        zero = path.mod != OneOrMore
        max_depth = 1 if path.mod == ZeroOrOne else closure_max_depth
        it = parse_property_path(subject, path.path, f'?{var_prefix}{0}', graph, dataset, forward, inverse, as_of)
        iterators = [it]
        for i in range(1, max_depth + 1):
            it = parse_property_path(f'?{var_prefix}{i - 1}', path.path, f'?{var_prefix}{i}', graph, dataset, forward, inverse, as_of)
            iterators.append(it)
        transitive_closure = TransitiveClosureIterator(subject, obj, zero, iterators, var_prefix, max_depth=max_depth)
        if zero:
            spo_pattern = {'subject': '?s', 'predicate': '?p', 'object': '?o', 'graph': graph}
            spo_scan = ScanIterator(spo_pattern, dataset, as_of=as_of)
            reflexive_closure = ReflexiveClosureIterator(subject, obj, spo_scan)
            return BagUnionIterator(transitive_closure, reflexive_closure)
        return transitive_closure
    elif type(path) is NegatedPath:
        print('negated property set')
        print(path.args)
        forward_properties = []
        for arg in path.args:
            if type(arg) is URIRef:
                forward_properties.append(str(arg))
            else:
                raise Exception('Negated property sets with reverse properties are not supported yet...')
        scan = parse_property_path(subject, f'?{var_prefix}{1}', obj, graph, dataset, forward, inverse, as_of)
        return FilterIterator(scan, create_equality_expr(f'?{var_prefix}{1}', forward_properties))
    elif type(path) is URIRef or type(path) is str:
        print('property')
        pattern = dict()
        pattern['subject'] = obj if inverse else subject 
        pattern['predicate'] = str(path)
        pattern['object'] = subject if inverse else obj
        pattern['graph'] = graph
        print(pattern)
        return ScanIterator(pattern, dataset, as_of=as_of)
    else:
        raise Exception('Unknwon expression type: ' + str(type(path)))
    

def build_path_pattern_iterator(triple: Dict[str, str], query_vars: List[str], dataset: Dataset, as_of: Optional[datetime] = None) -> PreemptableIterator:
    subject = triple['subject']
    path = triple['predicate']
    obj = triple['object']
    graph = triple['graph']
    if not subject.startswith('?') or subject in query_vars:
        pipeline = parse_property_path(subject, path, obj, graph, dataset, True, False, as_of)
    elif not obj.startswith('?') or obj in query_vars:
        pipeline = parse_property_path(obj, path, subject, graph, dataset, False, True, as_of)
    else:
        # pipeline = parse_property_path(obj, path, subject, graph, dataset, False, True, as_of)
        pipeline = parse_property_path(subject, path, obj, graph, dataset, True, False, as_of)
    return ProjectionIterator(pipeline, list(get_vars(triple)))


def build_scan_iterator(pattern: Dict[str, str], query_vars: List[str], dataset: Dataset, as_of: Optional[datetime] = None) -> PreemptableIterator:
    if pattern['selectivity'] == 0:
        return EmptyIterator()
    eq_expr, new_pattern = equality_variables(pattern['triple']['subject'], pattern['triple']['predicate'], pattern['triple']['object'])
    if eq_expr is not None:
        # copy pattern with rewritten values
        triple_pattern = dict()
        triple_pattern['subject'] = new_pattern[0]
        triple_pattern['predicate'] = new_pattern[1]
        triple_pattern['object'] = new_pattern[2]
        triple_pattern['graph'] = pattern['triple']['graph']
        # build a pipline with Index Scan + Equality filter
        if type(triple_pattern['predicate']) is str:
            pipeline = ScanIterator(triple_pattern, dataset, as_of=as_of)
        else:
            pipeline = build_path_pattern_iterator(triple_pattern, query_vars, dataset, as_of=as_of)
        pipeline = FilterIterator(pipeline, eq_expr)
    else:
        if type(pattern['triple']['predicate']) is str:
            pipeline = ScanIterator(pattern['triple'], dataset, as_of=as_of)
        else:
            pipeline = build_path_pattern_iterator(pattern['triple'], query_vars, dataset, as_of=as_of)
    return pipeline
    

def build_left_join_tree(bgp: List[Dict[str, str]], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> Tuple[PreemptableIterator, List[str], Dict[str, str]]:
    # gather metadata about triple patterns
    triples = []
    cardinalities = []

    # not common but happen in query insert where { bind }
    if len(bgp)==0:
        return EmptyIterator(),[],[]

    # analyze each triple pattern in the BGP
    for triple in bgp:
        # select the graph used to evaluate the pattern
        graph_uri = triple['graph'] if 'graph' in triple and len(triple['graph']) > 0 else default_graph
        triple['graph'] = graph_uri
        # get iterator and statistics about the pattern
        if type(triple['predicate']) is str:
            if dataset.has_graph(graph_uri):
                _, cardinality = dataset.get_graph(graph_uri).search(triple['subject'], triple['predicate'], triple['object'], as_of=as_of)
                selectivity = cardinality
            else:
                cardinality = 0
                selectivity = 0
            cardinalities += [{'triple': triple, 'cardinality': cardinality}]
        else: # Because property paths are not JSON serializable, we return no stats (need to be fix !)
            if dataset.has_graph(graph_uri):
                if triple['subject'].startswith('?') and triple['object'].startswith('?'):
                    selectivity = sys.maxsize
                else:
                    selectivity = -1
            else:
                selectivity = 0
        triples += [{'triple': triple, 'selectivity': selectivity}]
    
    # sort triples by ascending selectivity
    triples = sorted(triples, key=lambda v: v['selectivity'])
    
    # start the pipeline with the Scan with the most selective pattern
    pattern = triples.pop(0)
    query_vars = set()
    pipeline = build_scan_iterator(pattern, query_vars, dataset, as_of=as_of)
    query_vars = query_vars | get_vars(pattern['triple'])
    # build the left linear tree of joins
    while len(triples) > 0:
        pattern, pos, query_vars = find_connected_pattern(query_vars, triples)
        # no connected pattern = disconnected BGP => pick the first remaining pattern in the BGP
        if pattern is None:
            pattern = triples[0]
            pos = 0
        graph_uri = pattern['triple']['graph']
        scan = build_scan_iterator(pattern, query_vars, dataset, as_of=as_of)
        query_vars = query_vars | get_vars(pattern['triple'])
        pipeline = IndexJoinIterator(pipeline, scan)
        triples.pop(pos)
    return pipeline, query_vars, cardinalities

def continue_left_join_tree(iterator: PreemptableIterator, query_vars : List[str], bgp: List[Dict[str, str]], dataset: Dataset, default_graph: str, as_of: Optional[datetime] = None) -> Tuple[PreemptableIterator, List[str], Dict[str, str]]:
    """Build a Left-linear join tree from a Basic Graph pattern.

    Args:
      * bgp: Basic Graph pattern used to build the join tree.
      * dataset: RDF dataset on which the BGPC is evaluated.
      * default_graph: URI of the default graph used for BGP evaluation.
      * as_of: A timestamp used to perform all reads against a consistent version of the dataset. If `None`, use the latest version of the dataset, which does not guarantee snapshot isolation.

    Returns: A tuple (`iterator`, `query_vars`, `cardinalities`) where:
      * `iterator` is the root of the Left-linear join tree.
      * `query_vars` is the list of all SPARQL variables found in the BGP.
      * `cardinalities` is the list of estimated cardinalities of all triple patterns in the BGP.
    """
    # gather metadata about triple patterns
    triples = []
    cardinalities = []

    # analyze each triple pattern in the BGP
    for triple in bgp:
        # select the graph used to evaluate the pattern
        graph_uri = triple['graph'] if 'graph' in triple and len(triple['graph']) > 0 else default_graph
        triple['graph'] = graph_uri
        # get iterator and statistics about the pattern
        if dataset.has_graph(graph_uri):
            if type(triple['predicate']) is str:
                _, cardinality = dataset.get_graph(graph_uri).search(triple['subject'], triple['predicate'], triple['object'], as_of=as_of)
                selectivity = cardinality
            elif triple['subject'].startswith('?') and triple['object'].startswith('?'):
                cardinality = None
                selectivity = sys.maxsize
            else:
                cardinality = None
                selectivity = -1
        else:
            cardinality = 0
            selectivity = 0
        triples += [{'triple': triple, 'selectivity': selectivity}]
        cardinalities += [{'triple': triple, 'cardinality': cardinality}]
    
    # sort triples by ascending selectivity
    triples = sorted(triples, key=lambda v: v['selectivity'])

    pipeline=iterator

    # build the left linear tree of joins
    while len(triples) > 0:
        pattern, pos, query_vars = find_connected_pattern(query_vars, triples)
        # no connected pattern = disconnected BGP => pick the first remaining pattern in the BGP
        if pattern is None:
            pattern = triples[0]
            pos = 0
        graph_uri = pattern['triple']['graph']
        scan = build_scan_iterator(pattern, query_vars, dataset, as_of=as_of)
        query_vars = query_vars | get_vars(pattern['triple'])
        pipeline = IndexJoinIterator(pipeline, scan)
        triples.pop(pos)
    return pipeline, query_vars, cardinalities