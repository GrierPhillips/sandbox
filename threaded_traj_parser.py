from itertools import combinations
from time import time, sleep
import pickle
from collections import defaultdict
import multiprocessing as mp
from multiprocessing import Manager, Process
from multiprocessing.managers import BaseManager, ListProxy, DictProxy
from multiprocessing.sharedctypes import Array
from blist import blist  # pylint: disable=E0611
from ctypes import Structure, c_wchar_p


class ParseVehicleTrajectories(object):
    '''
    Class for processing vehicle trajectory .dat files and passing individual
    records on to worker processes.
    '''
    def __init__(self, filename, task_queue):
        self.filename = filename
        self.file_obj = None
        self.line = 0
        self.task_queue = task_queue

    @staticmethod
    def get_num_lines(line):
        '''
        Find the number of lines contained within a given vehicle record.
        '''
        line = line.replace('=', ' ').split()
        num_nodes = int(line[28])
        rem_lines = int((num_nodes - 1)/50)
        total_lines = (4 * rem_lines) + 4
        return total_lines

    @staticmethod
    def create_keys(info):
        '''
        Find all possible combinations of trips given the nodes traversed by
        a vehicle and the associated start time.
        '''
        stime, nodes, cum_time = info[0], info[4], info[5]
        node_pairs = [list(comb) for comb in combinations(nodes, 2)]
        for pair in node_pairs:
            node_idx = nodes.index(pair[0])
            # print(pair, node_idx, cum_time)
            pair.append(round(stime + float(cum_time[node_idx])))
        return node_pairs

    def run(self):
        '''
        Parse all records from a vehicle trajectory file into a dictionary of
        (stime, from_node, to_node) keys, with counts of each key as values.
        '''

        with open(self.filename, 'r') as self.file_obj:
            for _ in range(5):
                next(self.file_obj)
                self.line += 1
            for line in self.file_obj:
                self.line += 1
                if self.line > 2000:
                    break
                if line.startswith(' ##'):
                    break
                # if self.line > 27903916:
                #     import pdb; pdb.set_trace()
                record_size = self.get_num_lines(line)
                record = ''.join(
                    self.file_obj.readline() for _ in range(record_size))
                record = line + record
                self.line += record_size
                self.task_queue.put(record)
                if self.task_queue.full():
                    print(self.line)
                    sleep(5)


class TaskWorker(mp.Process):
    '''
    Worker process for handling records as they are added to the task queue.
    '''
    def __init__(self, task_queue, result_queue):
        mp.Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue

    def run(self):
        proc_name = self.name
        while True:
            record = self.task_queue.get()
            if record is None:
                print('{}: Exiting'.format(proc_name))
                self.task_queue.task_done()
                break
            node_pairs = self.process_record(record)
            self.task_queue.task_done()
            self.result_queue.put(node_pairs)
        return

    def process_record(self, record):
        '''
        Extract info from record and use it to collect list of (from_node,
        to_node, stime) tuples.
        '''
        info = self.parse_record(record)
        node_time_pairs = self.create_tuples(info)
        return node_time_pairs

    @staticmethod
    def parse_record(record):
        '''
        Parse an individual vehicle trajectory record.
        '''
        lines = record.rstrip().split('\n')
        line = lines[0].replace('=', ' ').split()
        stime = float(line[20])
        extra_lines = int((len(lines) - 1) / 5)
        if extra_lines > 0:
            nodes = ' '.join(lines[1:(1 + extra_lines)]).split()
            cum_time = ' '.join(
                lines[(2 + extra_lines):(2 + 2 * extra_lines)]).split()
        else:
            nodes = lines[1].split()
            cum_time = lines[2].split()
        return stime, nodes, cum_time

    @staticmethod
    def create_tuples(info):
        '''
        Find all possible combinations of trips given the nodes traversed by
        a vehicle and the associated start time.
        '''
        stime, nodes, cum_time = info[0], info[1], info[2]
        node_pairs = [list(comb) for comb in combinations(nodes, 2)]
        for pair in node_pairs:
            node_idx = nodes.index(pair[0])
            pair.append(round(stime + float(cum_time[node_idx])))
        return node_pairs


# class ResultWorker(mp.Process):
#     '''
#     Worker to handle results queue as node_time_pairs are added.
#     '''
#     def __init__(self, result_queue):
#         mp.Process.__init__(self)
#         self.result_queue = result_queue
#
#     def run(self):
#         '''
#         Get a list of nodes-time pairs and append the node pairs to the list
#         of times index corresponding to the start time of the trip.
#         '''
#         global times_list
#         proc_name = self.name
#         while True:
#             node_time_pairs = self.result_queue.get()
#             if node_time_pairs is None:
#                 print('{}: Exiting'.format(proc_name))
#                 self.result_queue.task_done()
#                 break
#             for f_node, t_node, stime in node_time_pairs:
#                 times_list[stime].append((f_node, t_node))
#             self.result_queue.task_done()


class Nodes(Structure):
    _fields_ = [('x', c_wchar_p), ('y', c_wchar_p)]


class TaskMaster(BaseManager):
    '''
    Subclass of BaseManager. Used to register blist for shared memory between
    processes.
    '''
    pass

TaskMaster.register('defaultdict', defaultdict, DictProxy)
TaskMaster.register('dict', dict, DictProxy)
TaskMaster.register('blist', blist, ListProxy)


def process_nodes(result_queue, times_list):
    while True:
        # st = time()
        node_time_pairs = result_queue.get()
        # print('get time: {}'.format(time() - st))
        if node_time_pairs is None:
            # print('{}: Exiting'.format(proc_name))
            result_queue.task_done()
            break
        for f_node, t_node, stime in node_time_pairs:
            # st = time()
            managed_obj_nested_add(times_list, stime, (f_node, t_node))
            # times_list[stime] += [(f_node, t_node)]
            # print('dict insert time: {}'.format(time() - st))
        result_queue.task_done()

def managed_obj_nested_add(obj, index, val):
    shared = obj[index]
    shared.add(val)
    obj[index] = shared

def main(num_cores):
    '''
    Main function. Starts worker processes, parses trajectory file, and creates
    node_times.pkl output with results.
    '''
    num_t_workers = int(2)
    num_r_workers = int(num_cores - num_t_workers)
    tasks = mp.JoinableQueue()
    results = mp.JoinableQueue()
    manager = TaskMaster()
    manager.start()
    # base_list = [list() for _ in range(2000)]
    partitions = [manager.defaultdict(set) for _ in range(num_r_workers)]
    # times = manager.blist()  # pylint: disable=E1101
    # for i in range(2000):
    #     for part in partitions:
    #         part[i] = set()
    # times = Array(Nodes, [[]]*2000)
    print(len(partitions))
    print('Creating {} task workers.'.format(num_t_workers))
    t_workers = make_workers(TaskWorker, num_t_workers, [tasks, results])
    print('Creating {} result workers.'.format(num_r_workers))
    # r_workers = make_workers(mp.Process, num_r_workers, [results])
    r_workers = []
    for i in range(num_r_workers):
        worker = mp.Process(target=process_nodes, args=(results, partitions[i]))
        worker.start()
        r_workers.append(worker)
    parser = ParseVehicleTrajectories('../git/LosManager/VehTrajectory_1.dat', tasks)
    start = time()
    parser.run()
    tasks.join()
    print('Record Parsing Time: {}'.format(time() - start))
    results.join()
    finish = time()
    print('Terminating task workers.')
    for worker in t_workers:
        worker.terminate()
    print('Terminating result workers.')
    for worker in r_workers:
        worker.terminate()
    final_dict = defaultdict(set)
    for part in partitions:
        for key, value in part.items():
            tups = list(value)
            for tup in tups:
                final_dict[key].add(tup)
    print('Total Processing Time: {} s'.format(finish - start))
    # import pdb; pdb.set_trace()
    # with open('node_times.pkl', 'wb') as file_obj:
    #     pickle.dump(times, file_obj)


def make_workers(classname, num, args):
    '''
    Create any number of workers for a given worker class.
    '''
    workers = []
    for _ in range(num):
        worker = classname(*args)
        workers.append(worker)
        worker.start()
    return workers


if __name__ == '__main__':
    main(mp.cpu_count() * 2)
