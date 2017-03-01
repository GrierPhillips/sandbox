'''
parse_trajectories_to_csv.py: This module allows a user to parse in a vehicle
trajecory file and parse it into a csv format. Output csv requeres ~ 50% of
original storage space.
'''

from time import time
from io import StringIO
import pandas as pd


class ParseVehicleTrajectories(object):
    '''
    Class for processing vehicle trajectory .dat files and passing individual
    records on to worker processes.
    '''
    def __init__(self, filename):
        self.filename = filename
        self.file_obj = None
        self.line = 0
        header = 'Veh #|Tag|OrigTaz|DestTaz|Class|Tck/Hov|UstmN|DownN|DestN|'\
            'Stime|TotalTime|# Nodes|VehType|EVAC|VOT|tFlag|PrefArrTime|'\
            'TripPur|IniGas|Toll|Nodes|CumTime|StepTime|DelayTime\n'
        self.csv = [header]

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
        val = self.file_obj.readline()
        self.line += 1
        for _ in range(rem_lines):
            val += self.file_obj.readline()
            self.line += 1
        return val.split()

    def add_to_csv(self, line):
        '''
        Find the number of lines contained within a given vehicle record.
        '''
        # st = time()
        # line = np.array(line.replace('=', ' ').replace('#', '').split())
        line = line.replace('=', ' ').replace('#', '').split()
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
        vals_string = '|'.join(vals)
        # print("| Join time: {}".format(time() - st2))
        self.line += 1
        rem_lines = int((num_nodes - 1)/50)
        import pdb; pdb.set_trace()
        for _ in range(4):
            # st2 = time()
            vals_string += '|' + str(self.iter_rem_lines(rem_lines))
            # print("vals {} time: {}".format(_, time() - st2))
        # st2 = time()
        self.csv.append(vals_string + '\n')
        # print('string extension time: {}'.format(time() - st2))
        # print('total time in function: {}'.format(time() - st))

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
                # if self.line % 100000 < 10:
                #     print(self.line)
                if self.line > 10:
                    break
                if line.startswith(' ##'):
                    break
                # st = time()
                self.add_to_csv(line)
                # print('Total Time: {}'.format(time() - st))


def main():
    '''
    Run parser and construct csv from trajectory records.

    Note: This is done with pandas.red_csv on a buffer of the final string as
    currently there are OS dependent issues related to writing files in excess
    of 2GB.
    '''
    parser = ParseVehicleTrajectories('VehTrajectory_1.dat')
    start = time()
    parser.run()
    print('Runtime of parser: {}'.format(time() - start))
    text = '\n'.join(parser.csv)
    data = pd.read_csv(StringIO(text), sep='|')
    data.to_csv('Parsed_Trajectories.csv', sep='|')  # pylint: disable=E1101
    # data.to_hdf('Parsed_Trajectories.hdf', 'trajectories', mode='w')


if __name__ == '__main__':
    main()
