
import ast
from time import time
from math import ceil
from itertools import repeat
from collections import Counter
from multiprocessing import cpu_count
import pandas as pd
import numpy as np

# pylint: disable=E1101


class RecordsAggregator(object):
    '''
    Class to be used for implementing a variety of aggregations on vehicle
    trajectory records.
    '''

    def __init__(self, records, tazs):
        self.reader, self.tazs = self._open_files(records, tazs)
        self.max_taz = self.tazs.tazList.max()[0]
        self.max_taz_length = 0
        self.tazs.tazList.map(self._get_max_length)
        self.sparse = self._make_sparse()
        self.tazs.apply(self._set_sparse, axis=1)
        self.final_value = None

    @staticmethod
    def _open_files(records, tazs):
        '''
        Read in a csv of vehicle trajectory records and a csv of Taz-Node
        associations.

        Args:
            records (string): Name of a .csv file containing vehicle trajectory
                records.
            tazs (string): Name of a .csv file containing Tazs associated with
                each node in the network.
        '''
        num_workers = cpu_count()
        chunksize = ceil(5225579 / num_workers)
        reader = pd.read_csv(
            records,
            sep=' ',
            engine='c',
            converters={
                'Nodes': ast.literal_eval},
            chunksize=chunksize)
        tazs = pd.read_csv(
            'nodetazs1.csv',
            sep='|',
            converters={'tazList': ast.literal_eval})
        tazs.tazList = tazs.tazList.map(np.array)
        return reader, tazs

    def _get_max_length(self, row):
        '''
        Set the maximum length of the lists in tazList.
        '''
        if len(row) > self.max_taz_length:
            self.max_taz_length = len(row)

    def _make_sparse(self):
        '''
        Construct an array of zeros that is shaped max_node x max_taz_length.
        The max_node value should be incremented slightly in case there are
        nodes in the network that do not show up in the vehicle records.
        '''
        max_node = self.tazs.tazList.node.max()
        sparse = np.zeros((max_node + 100, self.max_taz_length)).astype(int)
        return sparse

    def _set_sparse(self, row):
        '''
        Fill sparse array with array of tazs related to the node corresponding
        to that index number.
        '''
        self.sparse[row[0]][:row[2].shape[0]] = row[2]
        self.sparse[row[0]][row[2].shape[0]:] = 2108

    def _iterate_chunk(self, func):
        start = time()
        for chunk in self.reader:
            chunk.Nodes = chunk.Nodes.map(np.array)
            chunk = chunk.drop(chunk.columns[chunk.columns != 'nodes'], axis=1)
            chunk.Nodes.map(func)
        print('Total running time: {}'.format(time() - start))

    def count_tazs(self, arg):
        '''
        Setup the aggregator to count the number of trips that pass through
        each taz.
        '''
        self.final_value = np.zeros(self.max_taz + 2).astype(int)
        start = time()
        self._iterate_chunk(_count_tazs)


    def _count_tazs(row):
        '''
        For a given list of nodes, group them into bins and add the counts to the
        corresponding index in the final taz count.
        '''
        bins = np.bincount(self.sparse[row].flatten())
        self.final_value[:len(bins)] += bins



def collect_records_in_taz(row, taz_, sparse_, node_counts_):
    '''
    For a given list of nodes, if any of the nodes pass through the taz of
    interest pair all nodes in a tuple containing the node and a boolean for
    whether or not the node is in the taz of interest.

    Args:
        row: A row from a Pandas DataFrame object.
        taz_ (int): The taz number that you wish to aggregate nodes that
            have an interaction with.
        sparse_ (np.array): A numpy array populated with the taz numbers that
            each node interacts with.
        node_counts_ (Counter): A counter that keeps track of the occurances of
            each node bool pair in the form (node, bool) : count.
    '''
    tazs_ = sparse_[row][:, 0]
    tazs_[np.where(sparse[row] == 344)[0]] = taz_
    index_ = np.where(tazs_[np.where(tazs_ != 2108)] == taz_)
    if len(index_[0]) > 0:
        node_bool_pairs = np.array(list(map(np.array, zip(row, repeat(0)))))
        node_bool_pairs[index_, 1] += 1
        node_counts_.update(list(map(tuple, node_bool_pairs)))


if __name__ == '__main__':
    node_counts = Counter()
    start = time()
    for chunk in reader:
        chunk.Nodes = chunk.Nodes.map(np.array)
        chunk = chunk.drop(chunk.columns[chunk.columns != 'Nodes'], axis=1)
        # chunk.Nodes.map(lambda x: count_tazs(x, sparse, final_counts))
        chunk.Nodes.map(
            lambda x: collect_records_in_taz(x, 344, sparse, node_counts))

    # final_taz_bins = pd.DataFrame(final_counts)
    # final_taz_bins.to_csv('taz_bins.csv', index=False)
    counts = np.zeros((len(node_counts), 3)).astype(int)
    for index, (key, value) in enumerate(node_counts.items()):
        counts[index] = np.array([key[0], key[1], value])
    final_node_counts = pd.DataFrame(counts, columns=['Node', 'Bool', 'Count'])
    final_node_counts.to_csv('node_pair_counts.csv', index=False)
