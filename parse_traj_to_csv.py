'''
parse_trajectories_to_csv.py: This module allows a user to parse in a vehicle
trajecory file and parse it into a csv format. Output csv requeres ~ 50% of
original storage space.
'''

import mmap
import threading
from io import StringIO
import pandas as pd
import dask.bag as db
from toolz.itertoolz import partition_all
from dask.multiprocessing import get
import multiprocessing as mp
import time


class ParseVehicleTrajectories(object):
    '''
    Class for processing vehicle trajectory .dat files and passing individual
    records on to worker processes.
    '''
    def __init__(self, filename):
        self.filename = filename
        self.mem_map = None
        self.line = 0
        header = 'Veh #|Tag|OrigTaz|DestTaz|Class|Tck/Hov|UstmN|DownN|DestN|'\
            'Stime|TotalTime|# Nodes|VehType|EVAC|VOT|tFlag|PrefArrTime|'\
            'TripPur|IniGas|Toll|Nodes|CumTime|StepTime|DelayTime\n'
        self.csv = [header]
        self.records = mp.Queue()
        self.results = mp.Queue()

    def iter_rem_lines(self, rem_lines):
        '''
        Read in a line along with any number of remaining lines to be
        converted to an array of any data type (float, int, string).

        For example:
        If a record contains more than 50 nodes the information for nodes will
        encompass more than 1 line. Say a record has 75 nodes. This record will
        require 2 lines to display each of the nodes, cumulitive time, and
        delay time values. This function will read in the next line of a file
        object and if there are any remaining lines will concatenate those as
        well. Finally, this string will be converted into an array and
        converted using the given value of `conv` to set the final data type.

        "100100 100101 100102"
        iter_rem_lines(0, int)  --->    [100100, 100101, 100102]
        "0.19   1.32   2.56"
        iter_rem_lines(0, float)  --->    [0.19, 1.32, 2.56]
        '''
        val = self.mem_map.readline()
        # self.line += 1
        for _ in range(rem_lines):
            val += self.mem_map.readline()
            # self.line += 1
        line = b' '.join(val.split())
        return line

    def get_lines(self, line):
        num_nodes = int(line[161:166])
        total_lines = ((num_nodes - 1) // 50 + 1) * 4
        return total_lines

    def add_to_csv(self, line):
        '''
        Find the number of lines contained within a given vehicle record.
        '''
        # st = time()
        # line = np.array(line.replace('=', ' ').replace('#', '').split())
        line = line.replace(b'=', b' ').replace(b'#', b'').split()
        val_idx = list(range(1, 20, 2)) + [23] + list(range(26, 43, 2))
        # print('first step: {}'.format(time() - st))
        # st2 = time()
        vals = []
        for idx in val_idx:
            vals.append(line[idx])
        # vals = line[val_idx]
        # print('Numpy index time: {}'.format(time() - st2))
        num_nodes = int(vals[-9])
        # st2 = time()
        vals_string = b'|'.join(vals)
        # print("| Join time: {}".format(time() - st2))
        # self.line += 1
        rem_lines = int((num_nodes - 1)/50)
        # import pdb; pdb.set_trace()
        for _ in range(4):
            # st2 = time()
            vals_string += b'|' + self.iter_rem_lines(rem_lines)
            # print("vals {} time: {}".format(_, time() - st2))
        # st2 = time()
        self.csv.append((vals_string + b'\n').decode())
        # print('string extension time: {}'.format(time() - st2))
        # print('total time in function: {}'.format(time() - st))

    def run(self):
        '''
        Parse all records from a vehicle trajectory file into a dictionary of
        (stime, from_node, to_node) keys, with counts of each key as values.
        '''

        with open(self.filename, 'r') as file_obj:
            with mmap.mmap(file_obj.fileno(), 0, access=mmap.ACCESS_READ) as self.mem_map:
                for _ in range(5):
                    print(self.mem_map.readline())
                    # self.line += 1
                for line in iter(self.mem_map.readline, ''):
                    # self.line += 1
                    # if self.line % 100000 < 10:
                    #     print(self.line)
                    # if self.line > 10:
                    #     break
                    if line.startswith(b' ##'):
                        break
                    # st = time()
                    self.add_to_csv(line)
                    # lines = self.get_lines(line)
                    # rem_lines = []
                    # for _ in range(lines):
                    #     rem_lines.append(self.file_obj.readline())
                    # self.line += lines
                    # record = [line] + rem_lines
                    # self.records.put(record)
                    # if self.line % 100 == 0:
                    #     print(self.line)
                    # if self.records.full():
                        # print('Full', self.line)
                        # time.sleep(0.1)
        for _ in range(mp.cpu_count() * 5):
            self.records.put(None)
                # print('Total Time: {}'.format(time() - st))
        # print("Starting Dask")
        #
        # res = db.from_sequence(self.records, partition_size=1000).map_partitions(self.parse_each).compute(get=get)

        # for chunk in partition_all(1000, self.records)
        #     res = db.from_sequence(chunk)

    def run2(self):
        worker = mp.Process(target=self.run)
        worker.start()
        worker.join()

    def concat(self):
        while True:
            results = []
            for _ in range(100):
                if not self.results.empty():
                    results.append(self.results.get())
            threads = []
            for result in results:
                thread = threading.Thread(target=self.concat_one, args=(result,))
                threads.append(thread)
                thread.start()
            for thread in threads:
                thread.join()

    def concat_one(self, result):
        self.csv.append(result)

    def parse(self):
        workers = []
        for _ in range(mp.cpu_count() - 3):
            worker = mp.Process(target=self.parse_recs)
            workers.append(worker)
            worker.start()
        for _ in range(2):
            worker = mp.Process(target=self.concat)
            workers.append(worker)
            worker.start()
        workers.append(mp.Process(target=self.run))
        workers[-1].start()
        for worker in workers:
            worker.join()
        for worker in workers:
            worker.terminate()

    def parse_each(self, part):
        return list(map(self.parse_rec, part))

    def parse_recs(self):
        while True:
            records = []
            for _ in range(100):
                if not self.records.empty():
                    records.append(self.records.get())
            threads = []
            for record in records:
                thread = threading.Thread(target=self.parse_rec, args=(record,))
                threads.append(thread)
                thread.start()
            for thread in threads:
                thread.join()


    def parse_rec(self, record):
        header = record.pop(0)
        header = header.replace('=', ' ').replace('#', '').split()
        val_idx = list(range(1, 20, 2)) + [23] + list(range(26, 43, 2))
        vals = []
        for idx in val_idx:
            vals.append(header[idx])
        vals_string = '|'.join(vals)
        num_nodes = int(vals[-9])
        rem_lines = (((num_nodes - 1) // 50))
        for _ in range(4):
            vals_string += '|' + str(self.iter_rem_recs(record, rem_lines))
        self.results.put(vals_string)

    def iter_rem_recs(self, record, rem_lines):
        val = record.pop(0).strip()
        for _ in range(rem_lines):
            val += ' ' + record.pop(0).strip()
        return val.split()

def main():
    '''
    Run parser and construct csv from trajectory records.

    Note: This is done with pandas.red_csv on a buffer of the final string as
    currently there are OS dependent issues related to writing files in excess
    of 2GB.
    '''
    parser = ParseVehicleTrajectories('VehTrajectory_1.dat')
    start = time.time()
    parser.run()
    print('Runtime of parser: {}'.format(time.time() - start))
    text = '\n'.join(parser.csv)
    start = time.time()
    data = pd.read_csv(StringIO(text), sep='|')
    print('Runtime of loading csv: {}'.format(time.time() - start))
    data.info()
    import ipdb; ipdb.set_trace()
    # start = time.time()
    # data.to_csv('Parsed_Trajectories.csv', sep='|')  # pylint: disable=E1101
    # print('Runtime of saving csv: {}'.format(time.time() - start))
    start = time.time()
    data.to_hdf('Parsed_Trajectories.hdf', 'trajectories', mode='w')
    print('Runtime of saving hdfs: {}'.format(time.time() - start))


if __name__ == '__main__':
    main()
