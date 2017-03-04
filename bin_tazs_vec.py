import ast
import pickle
import ctypes
from time import time
import multiprocessing as mp
from multiprocessing import Manager
from multiprocessing.managers import BaseManager, DictProxy
from collections import defaultdict
from math import ceil
import pandas as pd
import numpy as np
import scipy.sparse as ss
# import matplotlib.pyplot as plt
# import seaborn as sns
from collections import Counter
from itertools import repeat
from contextlib import closing
# pylint: disable=E1101


def fill_sparse(row, sparse):
    for taz in row[2]:
        sparse[row[0], int(taz)] = taz


def count_tazs(row):
    bins = np.bincount(sparse[row])
    final_dict[:len(bins)] += bins


def set_sparse(row, sparse):
    sparse[row[0]] = row[2][0]


def process_chunk(chunk_item, sparse_mat, mgr_list):
    chunk_item.Nodes.map(lambda x: count_tazs(x, sparse_mat, mgr_list))


def init(shared_arr_, sparse_, final_dict_):
    global shared_arr
    global sparse
    global final_dict
    shared_arr = shared_arr_
    sparse = sparse_
    final_dict = final_dict_


def tonumpyarray(mp_arr):
    return np.frombuffer(mp_arr.get_obj())



class TaskMaster(BaseManager):
    '''
    Subclass of BaseManager. Used to register blist for shared memory between
    processes.
    '''
    pass

TaskMaster.register('defaultdict', defaultdict, DictProxy)




if __name__ == '__main__':
    num_workers = mp.cpu_count()
    chunksize = ceil(5225579 / num_workers)
    reader = pd.read_csv(
        'Parsed_Trajectories.csv',
        sep=' ',
        engine='c',
        converters={
            'Nodes': ast.literal_eval},
        chunksize=chunksize)
    tazs = pd.read_csv(
        'nodetazs1.csv',
        sep='|',
        converters={'tazList': ast.literal_eval})
    max_taz = tazs.tazList.max()[0]
    # sparse = ss.lil_matrix(np.zeros((300000, max_taz + 1)))
    # tazs.apply(lambda x: fill_sparse(x, sparse), axis=1)
    sparse = np.zeros(300000)
    sparse = sparse.astype(int)
    tazs.apply(lambda x: set_sparse(x, sparse), axis=1)
    # dicts = []
    # for _ in range(num_workers):
    #     dicts.append(sparse.copy())
    final_dict = np.zeros(2108)
    shared_arr = mp.Array(ctypes.c_double, 2108)
    arr = tonumpyarray(shared_arr)
    start = time()
    with closing(mp.Pool(initializer=init, initargs=(shared_arr, sparse, final_dict, ))) as pool:
        pool.map_async(count_tazs, reader)
    pool.join()
    # pool = mp.Pool(num_workers)
    # manager = TaskMaster()
    # manager.start()
    # manager_lists = [manager.defaultdict(int) for _ in range(num_workers)]
    # final_dict = manager.defaultdict(int)
    #workers = []

    # for chunk in reader:
    #     chunk.Nodes = chunk.Nodes.apply(np.array)
    #     chunk = chunk.drop(chunk.columns[chunk.columns != 'Nodes'], axis=1)
    #     chunk.Nodes.map(lambda x: count_tazs(x, sparse, final_dict))
    # pool.starmap_async(process_chunk, zip(reader, dicts, manager_lists))
    #for index, chunk in enumerate(reader):
    #    worker = pool.apply_async(process_chunk, [chunk, sparse, manager_lists[index]])
    #    workers.append(worker)
    # print('Workers have started')
    # pool.close()
    # print('Pool is closed')
    # pool.join()
    # print('Pool has joined')
    print('Total running time in pool: {}'.format(time() - start))
    final_taz_bins = pd.DataFrame(final_dict)
    # for part in manager_lists:
    #     part = pd.Series(part.values(), index=part.keys())
    #     final_taz_bins += part
    import pdb; pdb.set_trace()
    # final_taz_bins = pd.DataFrame(final_taz_bins)
    final_taz_bins.to_csv('taz_bins.csv', index=False)
