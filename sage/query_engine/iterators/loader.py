# loader.py
# Author: Thomas MINIER - MIT License 2017-2020
from datetime import datetime
from typing import Dict, Optional, Union

from sage.database.core.dataset import Dataset
from sage.query_engine.iterators.filter import FilterIterator
from sage.query_engine.iterators.bind import BindIterator
from sage.query_engine.iterators.construct import ConstructIterator
from sage.query_engine.iterators.transitive_closure import TransitiveClosureIterator
from sage.query_engine.iterators.reflexive_closure import ReflexiveClosureIterator
from sage.query_engine.iterators.nlj import IndexJoinIterator
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.iterators.projection import ProjectionIterator
from sage.query_engine.iterators.reduced import ReducedIterator
from sage.query_engine.iterators.scan import ScanIterator
from sage.query_engine.iterators.union import BagUnionIterator
from sage.query_engine.protobuf.iterators_pb2 import (RootTree,
                                                      SavedBagUnionIterator,
                                                      SavedFilterIterator,
                                                      SavedTransitiveClosureIterator,
                                                      SavedReflexiveClosureIterator,
                                                      SavedIndexJoinIterator,
                                                      SavedProjectionIterator,
                                                      SavedReducedIterator,
                                                      SavedScanIterator,
                                                      SavedBindIterator,
                                                      SavedConstructIterator)
from sage.query_engine.protobuf.utils import protoTriple_to_dict

import sys, traceback
import logging

##1
## Don't forget to add your saved iterator here !!
## If you add one ....
###
SavedProtobufPlan = Union[RootTree,SavedBagUnionIterator,SavedFilterIterator,SavedIndexJoinIterator,SavedProjectionIterator,SavedScanIterator,SavedBindIterator,SavedConstructIterator,SavedReducedIterator]


def load(saved_plan: SavedProtobufPlan, dataset: Dataset) -> PreemptableIterator:
    """Load a preemptable physical query execution plan from a saved state.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    # unpack the plan from the serialized protobuf message
    try:
#        print(f"...{type(saved_plan)}...")
        if isinstance(saved_plan, bytes):
            root = RootTree()
            root.ParseFromString(saved_plan)
            sourceField = root.WhichOneof('source')
            saved_plan = getattr(root, sourceField)
        # load the plan based on the current node
        if type(saved_plan) is SavedFilterIterator:
            return load_filter(saved_plan, dataset)
        elif type(saved_plan) is SavedProjectionIterator:
            return load_projection(saved_plan, dataset)
        elif type(saved_plan) is SavedReducedIterator:
            return load_reduced(saved_plan, dataset)
        elif type(saved_plan) is SavedScanIterator:
            return load_scan(saved_plan, dataset)
        elif type(saved_plan) is SavedIndexJoinIterator:
            return load_nlj(saved_plan, dataset)
        elif type(saved_plan) is SavedBagUnionIterator:
            return load_union(saved_plan, dataset)
        elif type(saved_plan) is SavedBindIterator:
            return load_bind(saved_plan, dataset)
        elif type(saved_plan) is SavedConstructIterator:
            return load_construct(saved_plan, dataset)
        elif type(saved_plan) is SavedTransitiveClosureIterator:
            return load_transitive_closure(saved_plan, dataset)
        elif type(saved_plan) is SavedReflexiveClosureIterator:
            return load_reflexive_closure(saved_plan, dataset)
        else:
            raise Exception(f"Unknown iterator type '{type(saved_plan)}' when loading controls")
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        logging.error(f"load_plan:{sys.exc_info()[0]}")
        raise


def load_projection(saved_plan: SavedProjectionIterator, dataset: Dataset) -> PreemptableIterator:
    """Load a ProjectionIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset)
    values = saved_plan.values if len(saved_plan.values) > 0 else None
    return ProjectionIterator(source, values)

def load_reduced(saved_plan: SavedReducedIterator, dataset: Dataset) -> PreemptableIterator:
    """Load a ReducedIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset)
    return ReducedIterator(source)



def load_filter(saved_plan: SavedFilterIterator, dataset: Dataset) -> PreemptableIterator:
    """Load a FilterIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset)
    mu = None
    if len(saved_plan.mu) > 0:
        mu = saved_plan.mu
    return FilterIterator(source, saved_plan.expression, mu=mu)

def load_bind(saved_plan: SavedBindIterator, dataset: Dataset) -> PreemptableIterator:
    """Load a BindIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    sourceField = saved_plan.WhichOneof('source')
    source = None
    #print("sourcefield:"+str(sourceField))
    if sourceField is not None:
        source = load(getattr(saved_plan, sourceField), dataset)

    mu = None
    if len(saved_plan.mu) > 0:
        mu = saved_plan.mu
    return BindIterator(source, saved_plan.bindexpr,saved_plan.bindvar, mu=mu, delivered=saved_plan.delivered)


def load_construct(saved_plan: SavedConstructIterator, dataset: Dataset) -> PreemptableIterator:
    """Load a ConstructIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    sourceField = saved_plan.WhichOneof('source')
    source = load(getattr(saved_plan, sourceField), dataset)
    # saved as a list of triplePattern Objects, but Iterator waits for a list of tuple
    template=[]
    for tp in saved_plan.template:
        template.append( (tp.subject,tp.predicate,tp.object) )
    return ConstructIterator(source, template)


def load_scan(saved_plan: SavedScanIterator, dataset: Dataset) -> PreemptableIterator:
    """Load a ScanIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    pattern = protoTriple_to_dict(saved_plan.pattern)
    if saved_plan.timestamp is not None:
        if saved_plan.timestamp == '':
            as_of = None
        else:
            as_of = datetime.fromisoformat(saved_plan.timestamp)
    else:
        as_of = None
    current_binding = None
    if len(saved_plan.mu) > 0:
      current_binding = saved_plan.mu
    return ScanIterator(pattern, dataset, current_binding=current_binding, cardinality=saved_plan.cardinality, progress=saved_plan.progress, last_read=saved_plan.last_read, as_of=as_of)


def load_transitive_closure(saved_plan: SavedTransitiveClosureIterator, dataset: Dataset) -> PreemptableIterator:
    """Load a TransitiveClosureIterator from a protobuf serialization
    
    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    subject = saved_plan.subject
    obj = saved_plan.obj
    var_prefix = saved_plan.var_prefix
    current_depth = saved_plan.current_depth
    min_depth = saved_plan.min_depth
    max_depth = saved_plan.max_depth
    complete = saved_plan.complete

    iterators = []
    for iterator in saved_plan.iterators:
      it_field = iterator.WhichOneof('iterator')
      iterators.append(load(getattr(iterator, it_field), dataset))

    bindings = []
    for binding in saved_plan.bindings:
      bindings.append(getattr(binding, 'binding'))
    bindings += [None] * ( (max_depth + 1) - len(bindings) )
    
    return TransitiveClosureIterator(subject, obj, iterators, var_prefix, bindings, current_depth, min_depth, max_depth, complete)


def load_reflexive_closure(saved_plan: SavedTransitiveClosureIterator, dataset: Dataset) -> PreemptableIterator:
    """Load a TransitiveClosureIterator from a protobuf serialization
    
    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    subject = saved_plan.subject
    obj = saved_plan.obj
    source = load(saved_plan.scan_source, dataset)
    current_binding = None
    if len(saved_plan.current_binding) > 0:
        current_binding = saved_plan.current_binding
    mu = None
    if len(saved_plan.mu) > 0:
        mu = saved_plan.mu
    done = saved_plan.done 
    return ReflexiveClosureIterator(subject, obj, source, mu, current_binding, done)


def load_nlj(saved_plan: SavedIndexJoinIterator, dataset: Dataset) -> PreemptableIterator:
    """Load a IndexJoinIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    leftField = saved_plan.WhichOneof('left')
    left = load(getattr(saved_plan, leftField), dataset)
    rightField = saved_plan.WhichOneof('right')
    right = load(getattr(saved_plan, rightField), dataset)
    current_binding = None
    if len(saved_plan.mu) > 0:
        current_binding = saved_plan.mu
    return IndexJoinIterator(left, right, current_binding=current_binding)



def load_union(saved_plan: SavedBagUnionIterator, dataset: Dataset) -> PreemptableIterator:
    """Load a BagUnionIterator from a protobuf serialization.

    Args:
      * saved_plan: Saved query execution plan.
      * dataset: RDF dataset used to execute the plan.

    Returns:
      The pipeline of iterator used to continue query execution.
    """
    leftField = saved_plan.WhichOneof('left')
    left = load(getattr(saved_plan, leftField), dataset)
    rightField = saved_plan.WhichOneof('right')
    right = load(getattr(saved_plan, rightField), dataset)
    return BagUnionIterator(left, right)
