from sage.query_engine.exceptions import TooManyResults
from math import inf
import logging, xxhash, json

class ControlTuplesBuffer(object):

    def __init__(self, max_control_tuples=inf):
        super(ControlTuplesBuffer, self).__init__()
        self._max_control_tuples = max_control_tuples
        self._frontier_nodes = dict()
        self._control_tuples = dict()
        self._size = 0


    def create_control_tuple(self, context, node, depth, max_depth, forward, source, pattern, destination):
        control_tuple = dict()
        control_tuple['path'] = dict()
        control_tuple['context'] = context
        control_tuple['node'] = node
        control_tuple['depth'] = depth
        control_tuple['max_depth'] = max_depth
        control_tuple['forward'] = forward
        control_tuple['path']['subject'] = source
        control_tuple['path']['predicate'] = pattern
        control_tuple['path']['object'] = destination
        return control_tuple


    def _isFrontierNode(self, control_tuple):
        return control_tuple['depth'] == control_tuple['max_depth']


    def add(self, path_pattern_id, control_tuple):
        ptc_id = xxhash.xxh64(f'{path_pattern_id}{json.dumps(control_tuple["context"])}').hexdigest()
        if ptc_id not in self._control_tuples:
            self._control_tuples[ptc_id] = dict()
            self._frontier_nodes[ptc_id] = 0
        node = control_tuple['node']
        if node not in self._control_tuples[ptc_id]:
            self._control_tuples[ptc_id][node] = control_tuple
            self._size += 1
            if self._isFrontierNode(control_tuple):
                self._frontier_nodes[ptc_id] += 1
            logging.info(f'New control tuple for the PTC {ptc_id}: {self._size} control tuples now !')
        elif not self._isFrontierNode(control_tuple) and self._isFrontierNode(self._control_tuples[ptc_id][node]):
            self._control_tuples[ptc_id][node]['depth'] = control_tuple['depth']
            self._frontier_nodes[ptc_id] -= 1
            logging.info(f'A frontier node has been discarded !!!')
        else:
            logging.info(f'Saving one control tuple !')
        if self._size > self._max_control_tuples:
            logging.info('Too many control tuples !!!')
            raise TooManyResults()


    def _compress_control_tuples(self, ptc_id):
        compact_control_tuple = None
        for control_tuple in self._control_tuples[ptc_id].values():
            if compact_control_tuple is None:
                compact_control_tuple = control_tuple
                compact_control_tuple['nodes'] = list()
            compact_control_tuple['nodes'].append({
                'node': control_tuple['node'],
                'depth': control_tuple['depth']
            })
        del compact_control_tuple['node']
        del compact_control_tuple['depth']
        return compact_control_tuple


    def collect(self, oneQuantumQuery):
        control_tuples = list()
        for ptc_id in self._control_tuples.keys():
            if not oneQuantumQuery or self._frontier_nodes[ptc_id] > 0:
                control_tuples.append(self._compress_control_tuples(ptc_id))
        logging.info(f'Collecting {self._size} control tuples')
        return control_tuples