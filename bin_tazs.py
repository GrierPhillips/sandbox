import ast
import pickle
import multiprocessing as mp
from multiprocessing import Manager
from multiprocessing.managers import BaseManager, DictProxy
from collections import defaultdict
from math import ceil
import pandas as pd
import numpy as np
import scipy.sparse as ss
import dask.dataframe as dd

# import matplotlib.pyplot as plt
# import seaborn as sns
from collections import Counter
# pylint: disable=E1101


def fill_sparse(row, sparse):
    for taz in row[2]:
        sparse[row[0], int(taz)] = taz


def count_tazs(row, sparse_mat, mgr_list):
    print(row)
    for node in row.Nodes:
        for taz in sparse_mat[int(node)].data[0]:
            mgr_list[int(taz)] += 1


def process_chunk(df, sparse_mat, mgr_list):
    print(chunk_item, sparse_mat, )
    df.Nodes.apply(lambda x: count_tazs(x, sparse_mat, mgr_list))


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
    dask_df = dd.read_csv(
        'Parsed_Trajectories.csv',
        sep=' ',
        converters={
            'Nodes': ast.literal_eval})
    # reader = pd.read_csv(
    #     'Parsed_Trajectories.csv',
    #     sep=' ',
    #     engine='c',
    #     converters={
    #         'Nodes': ast.literal_eval},
    #     chunksize=chunksize)
    tazs = pd.read_csv(
        'nodetazs1.csv',
        sep='|',
        converters={'tazList': ast.literal_eval})

    sparse = ss.lil_matrix(np.zeros((300000, tazs.tazList.max()[0] + 1)))
    tazs.apply(lambda x: fill_sparse(x, sparse), axis=1)
    # context = mp.get_context('spawn')
    pool = mp.Pool(num_workers)
    manager = TaskMaster()
    manager.start()
    final_dict = manager.defaultdict(int)
    # manager_lists = [manager.defaultdict(int) for _ in range(num_workers)]
    # workers = []
    # for index, chunk in enumerate(reader):
    #     worker = pool.apply_async(process_chunk, [chunk, sparse, manager_lists[index]])
    #     workers.append(worker)
    result = dask_df.apply(
        count_tazs,
        axis=1,
        args=(sparse, final_dict,),
        meta=({
            'Veh#': 'i8',
            'Tag': 'i8',
            'OrigTaz': 'i8',
            'DestTaz': 'i8',
            'Class': 'i8',
            'Tck/Hov': 'i8',
            'UstmN': 'i8',
            'DownN': 'i8',
            'DestN': 'i8',
            'Stime': 'f8',
            'TotalTime': 'f8',
            '#Nodes': 'i8',
            'VehType': 'i8',
            'EVAC': 'i8',
            'VOT': 'f8',
            'tFlag': 'i8',
            'PrefArrTime': 'f8',
            'TripPur': 'i8',
            'IniGas': 'f8',
            'Toll': 'f8',
            'Nodes': 'object'}))
    # final_taz_bins = pd.Series(0, index=np.arange(tazs.tazList.max()[0] + 1))
    # for part in manager_lists:
    #     part = pd.Series(part.values(), index=part.keys())
    #     final_taz_bins += part

    import pdb; pdb.set_trace()

    final_dict.to_csv('taz_bins.csv', index=False)
