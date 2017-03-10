import ast
from math import ceil
from time import time
import pandas as pd
import numpy as np
import dask.dataframe as dd
import dask
from multiprocessing import cpu_count
from dask.multiprocessing import get

# pylint: disable=E1101



def count_tazs(row, sparse_, final_arr_):
    bins = np.bincount(sparse_[row.Nodes].flatten())
    final_arr_[:len(bins)] += bins

def set_sparse(row, sparse_):
    sparse_[row[0]][:row[3].shape[0]] = row[3]
    sparse_[row[0]][row[3].shape[0]:] = 2108

def process_chunk(chunk_item, sparse_mat, mgr_list):
    chunk_item.Nodes.map(lambda x: count_tazs(x, sparse_mat, mgr_list))

def add1(df):
    return df[df.columns[0]] + 1

def myadd(df, a, b=1):
    return df.x + df.y + a + b


if __name__ == '__main__':
    num_workers = cpu_count()
    chunksize = ceil(5225579 / num_workers)
    dask_df = dd.read_csv(
        'Parsed_Trajectories.csv',
        sep=' ',
        converters={
            'Nodes': ast.literal_eval})
    # df = pd.DataFrame({'x': [1, 2, 3, 4, 5],
    #                    'y': [1., 2., 3., 4., 5.]})
    # df.to_csv('test.csv')
    # ddf = dd.read_csv('test.csv')
    # res = ddf.map_partitions(myadd, 1, b=2).compute(get=get)
    res = dask_df.map_partitions(add1).compute(get=get)
    # reader = pd.read_csv(
    #     'Parsed_Trajectories.csv',
    #     sep=' ',
    #     engine='c',
    #     converters={
    #         'Nodes': ast.literal_eval},
    #     chunksize=chunksize)
    # tazs = dd.read_csv(
    #     'nodetazs1.csv',
    #     sep='|',
    #     converters={'tazList': ast.literal_eval})
    # print(tazs.head())
    # # max_taz = tazs.tazList.max()[0]
    # tazs['nodes2'] = 0
    # tazs['nodes2'] = tazs.map_partitions(add1).compute(get=get)
    # # tazs['nodes2'] = res
    # import pdb; pdb.set_trace()
    # sparse = np.zeros((300000, 7)).astype(int)
    # tazs.apply(lambda x: set_sparse(x, sparse), axis=1)
    # final_arr = np.zeros(2109)
    # start = time()
    # import pdb; pdb.set_trace()
    # dask_df.Nodes = dask_df.Nodes.map(np.array)
    # dask_df = dask_df.drop(dask_df.columns[dask_df.columns != 'Nodes'], axis=1)
    # result = dask_df.map(
    #     count_tazs,
    #     axis=1,
    #     args=(sparse, final_arr,),
    #     meta=({
    #         'Veh#': 'i8',
    #         'Tag': 'i8',
    #         'OrigTaz': 'i8',
    #         'DestTaz': 'i8',
    #         'Class': 'i8',
    #         'Tck/Hov': 'i8',
    #         'UstmN': 'i8',
    #         'DownN': 'i8',
    #         'DestN': 'i8',
    #         'Stime': 'f8',
    #         'TotalTime': 'f8',
    #         '#Nodes': 'i8',
    #         'VehType': 'i8',
    #         'EVAC': 'i8',
    #         'VOT': 'f8',
    #         'tFlag': 'i8',
    #         'PrefArrTime': 'f8',
    #         'TripPur': 'i8',
    #         'IniGas': 'f8',
    #         'Toll': 'f8',
    #         'Nodes': 'object'})).compute(get=get)
    # print('Total running time: {}'.format(time() - start))
    import pdb; pdb.set_trace()
    #
    # final_dict.to_csv('taz_bins.csv', index=False)
