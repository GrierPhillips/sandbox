from collections import defaultdict, Counter
from itertools import combinations
from time import time, sleep
# import pickle as pickle
import multiprocessing as mp
import threading
from multiprocessing.managers import BaseManager, DictProxy


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
                if self.line > 10000:
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
        info = self.parse_record(record)
        node_pairs = self.create_keys(info)
        return node_pairs

    def parse_record(self, record):
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

    def create_keys(self, info):
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

class ProcessManager(object):
    def __init__(self):
        self.workers = []
        self.errors_flag = False
        self._threads = []
        self._lock = threading.Lock()

    def terminate_all(self):
        with self._lock:
            for worker in self.workers:
                if worker.is_alive():
                    print('Terminating {}'.format(worker.name))
                    worker.terminate()

    def launch_proc(self, func, args=()):
        thread = threading.Thread(
            target=self._worker_thread_runner, args=(func, args))
        self._threads.append(thread)
        thread.start()

    def _worker_thread_runner(self, func, args):
        worker = func(args)
        self.workers.append(worker)
        worker.start()
        while worker.exitcode is None:
            worker.join()
        if worker.exitcode > 0:
            self.errors_flag = True
            self.terminate_all()

    def wait(self):
        for thread in self._threads:
            thread.join()


class ResultWorker(mp.Process):
    '''
    Worker to handle results queue as node_pairs are added.
    '''
    def __init__(self, result_queue, return_dict):
        mp.Process.__init__(self)
        self.result_queue = result_queue
        self.key_counts = defaultdict(int)
        self.return_dict = return_dict

    def run(self):
        proc_name = self.name
        key_counts = defaultdict(int)
        while True:
            node_pairs = self.result_queue.get()
            if node_pairs is None:
                print('{}: Exiting'.format(proc_name))
                self.result_queue.task_done()
                break
            new_vals = self.process_nodes(node_pairs)
            self.result_queue.task_done()
            self.return_dict.update(new_vals)
        # print(self.return_dict)
        return


def process_nodes(results_queue, return_dict):
    # import pdb; pdb.set_trace()
    while True:
        # if results_queue.empty():
        #     sleep(0.5)
        # st = time()
        node_pairs = results_queue.get()
        # print('get time: {}'.format(time() - st))
        if node_pairs is None:
            results_queue.task_done()
            break
        for key in node_pairs:
            # st = time()
            return_dict[tuple(key)] += 1
            # print('dict insert time: {}'.format(time() - st))
        results_queue.task_done()
        # print(return_dict)


class MyManager(BaseManager):
    pass

MyManager.register('defaultdict', defaultdict, DictProxy)
MyManager.register('Counter', Counter, DictProxy)


if __name__ == '__main__':
    # mpl = mp.log_to_stderr()
    # mpl.setLevel(mp.SUBDEBUG)
    num_workers = mp.cpu_count() * 2 - 1
    tasks = mp.JoinableQueue()
    results = mp.JoinableQueue()
    # key_counts = defaultdict(int)
    print('Creating {} task workers'.format(num_workers))
    parser = ParseVehicleTrajectories('VehTrajectory_1.dat', tasks)
    task_workers = [TaskWorker(tasks, results) for _ in range(1)]
    for worker in task_workers:
        worker.start()
    manager = MyManager()
    manager.start()
    partitions = [manager.defaultdict(int) for _ in range(num_workers)]
    # for i in range(1):
    # import pdb; pdb.set_trace()
    result_workers = []
    for i in range(num_workers):
        result_workers.append(
            mp.Process(target=process_nodes, args=(results, partitions[i])))
    # results_worker = mp.Process(target=process_nodes, args=(results, return_dict))
    # results_worker.start()
    for worker in result_workers:
        worker.start()

    # pool = mp.Pool(1, process_nodes, (results, return_dict,))
    # results_worker = ResultWorker(results, return_dict)
    # key_counts = results_worker.start()
    # pool = mp.Pool(processes=1)
    start = time()
    parser.run()
    # for _ in task_workers:
    #     tasks.put(None)
    # sleep(0.5)
    tasks.join()
    print('Record Parsing Time: {}'.format(time() - start))
    results.join()
    for worker in task_workers:
        worker.terminate()
    for worker in result_workers:
        worker.terminate()
    final_dict = Counter()
    st = time()
    for part in partitions:
        final_dict += part
    finish = time()
    print('Time to combine dictionaries: {}'.format(finish - st))
    # pool.close()
    # pool.join()
    # sleep(2)
    print('Total Processing Time: {} s'.format(finish - start))
    # import pdb; pdb.set_trace()
    # sleep(1)
    print(len(final_dict), sum(final_dict.values()))
    # import pdb; pdb.set_trace()
    # with open('key_counts.pkl', 'w') as f:
    #     pickle.dump(f)
