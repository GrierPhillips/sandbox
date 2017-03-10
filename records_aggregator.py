
import ast
from time import time
from math import ceil
from itertools import repeat
from collections import Counter
from multiprocessing import cpu_count
import pandas as pd
import numpy as np

# pylint: disable=E1101


def count_tazs(row, sparse_, final_counts_):
    '''
    For a given list of nodes, group them into bins and add the counts to the
    corresponding index in the final taz count.
    '''
    bins = np.bincount(sparse_[row].flatten())
    final_counts_[:len(bins)] += bins


def set_sparse(row, sparse_):
    '''
    Fill sparse array with array of tazs related to the node corresponding
    to that index number.
    '''
    sparse_[row[0]][:row[2].shape[0]] = row[2]
    sparse_[row[0]][row[2].shape[0]:] = 2108


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
    num_workers = cpu_count()
    chunksize = ceil(5225579 / num_workers)
    reader = pd.read_csv(
        'Parsed_Trajectories.csv',
        sep=' ',
        engine='c',
        converters={
            'Nodes': ast.literal_eval},
        chunksize=chunksize)
    # dask_df = dd.read_csv(
    #     'Parsed_Trajectories.csv',
    #     sep=' ',
    #     converters={
    #         'Nodes': ast.literal_eval})
    tazs = pd.read_csv(
        'nodetazs1.csv',
        sep='|',
        converters={'tazList': ast.literal_eval})
    max_taz = tazs.tazList.max()[0]
    tazs.tazList = tazs.tazList.map(np.array)
    sparse = np.zeros((300000, 7)).astype(int)
    tazs.apply(lambda x: set_sparse(x, sparse), axis=1)
    final_counts = np.zeros(2109).astype(int)
    node_counts = Counter()
    start = time()
    for chunk in reader:
        chunk.Nodes = chunk.Nodes.map(np.array)
        chunk = chunk.drop(chunk.columns[chunk.columns != 'Nodes'], axis=1)
        # chunk.Nodes.map(lambda x: count_tazs(x, sparse, final_counts))
        chunk.Nodes.map(
            lambda x: collect_records_in_taz(x, 344, sparse, node_counts))
    print('Total running time: {}'.format(time() - start))
    # final_taz_bins = pd.DataFrame(final_counts)
    # final_taz_bins.to_csv('taz_bins.csv', index=False)
    counts = np.zeros((len(node_counts), 3)).astype(int)
    for index, (key, value) in enumerate(node_counts.items()):
        counts[index] = np.array([key[0], key[1], value])
    final_node_counts = pd.DataFrame(counts, columns=['Node', 'Bool', 'Count'])
    final_node_counts.to_csv('node_pair_counts.csv', index=False)
