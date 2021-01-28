from sage.query_engine.exceptions import TooManyResults
from math import inf
import logging

class ControlTuplesBuffer(object):

    def __init__(self, max_control_tuples=inf):
        super(ControlTuplesBuffer, self).__init__()
        self._max_control_tuples = max_control_tuples
        self._temp = dict()
        self._temp_size = 0
        self._buffer = list()

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

    def flush(self, identifiant):
        if identifiant in self._temp:
            self._buffer.extend(self._temp[identifiant])
            self._temp_size -= len(self._temp[identifiant])
            logging.info(f'Flushing {len(self._temp[identifiant])} control tuples [{identifiant}]')
            self._temp[identifiant] = list()

    def flush_all(self):
        logging.info('Flushing all')
        for identifiant in self._temp:
            self.flush(identifiant)

    def clear(self, identifiant):
        if identifiant in self._temp:
            self._temp_size -= len(self._temp[identifiant])
            logging.info(f'Clearing {len(self._temp[identifiant])} control tuples [{identifiant}]')
            self._temp[identifiant] = list()

    def add(self, identifiant, control_tuple):
        if identifiant not in self._temp:
            self._temp[identifiant] = list()
        self._temp[identifiant].append(control_tuple)
        self._temp_size += 1
        logging.info(f'Adding a control tuple [{identifiant}]: {self._temp_size + len(self._buffer)} control tuples now !')
        if (self._temp_size + len(self._buffer)) > self._max_control_tuples:
            logging.info('Too many control tuples !!!')
            raise TooManyResults()

    def collect(self):
        self.flush_all()
        logging.info(f'Collecting {len(self._buffer)} control tuples')
        control_tuples = self._buffer
        self._buffer = list()
        return control_tuples