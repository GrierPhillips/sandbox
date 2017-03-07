#
# C10 ABM-DTA Integration
# DynusT batch run for iSAM

import os
import struct
from array import array
from shutil import copyfile, SameFileError
import numpy as np


class AB_DST2(object):
    def __init__(self, params_file):
        self.params = {}
        self.get_params(params_file)
        self.header_info = {}
        self.value_info = {}
        self.vehicle_id = []
        self.total_time = []
        self.tag = []
        self.record = 0
        # self.run()

    def _populate_fmts(self):
        '''
        Set values for header_info and value_info.
        '''
        fmt_header = '<iBHHBBxxxiiiffiBBfBfBffx'
        size_header = struct.calcsize(fmt_header)  # type: int
        self.header_info = {'fmt': fmt_header, 'size': size_header}
        fmt_value = '<i'
        size_value = struct.calcsize(fmt_value)
        self.value_info = {'fmt': fmt_value, 'size': size_value}

    def get_params(self, params_file):
        '''
        Get paramaters for running DynusT
        '''
        with open(params_file, 'r') as file_obj:
            for line in file_obj:
                try:
                    param, value = line.split('=')
                except ValueError:
                    continue
                self.params[param] = value.strip()

    def prepare_vehicle(self):
        inf = self.params["in_vehicle"]
        ouf = self.params["in_folder"] + 'vehicle.dat'
        print("Preparing vehicle.dat...")
        try:
            copyfile(inf, ouf)
        except SameFileError:
            print('Failed in preparing vehicle.dat!\nThe destination file' +
                  ' is the same as source.')
        except OSError:
            print('Failed in preparing vehicle.dat!\nYou do not have write ' +
                  'access for the destination file.')

    def prepare_path(self):
        inf = self.params["in_path"]
        ouf = self.params["in_folder"] + "path.dat"
        print("Preparing path.dat...")
        try:
            copyfile(inf, ouf)
        except SameFileError:
            print('Failed in preparing path.dat!\nThe destination file' +
                  ' is the same as source.')
        except OSError:
            print('Failed in preparing path.dat!\nYou do not have write ' +
                  'access for the destination file.')

    def prepare_dynust(self):
        with open(self.params["in_folder"] + "system.dat", 'w') as file_obj:
            horizon = int(self.params["horizon"])
            iteration = int(self.params["iters"])
            if self.params["method"] == "cold":
                method = 0
            else:
                method = 2
            agginterval = 50
            rgap = 100
            print("Preparing DynusT...")
            text = '{}\n'.format(horizon)
            text += '{} {}\n'.format(iteration, method)
            text += '{} {} {} {}\n'.format(agginterval, agginterval, 0, rgap)
            file_obj.write(text)
        with open(self.params["in_folder"] + "parameter.dat", 'r') as file_obj:
            lines = file_obj.readlines()
        line = lines[36].split("=")
        lines[36] = '{} = {}'.format(int(self.params["threads"]), line[1])
        split_lines = [64, 65]
        for index in split_lines:
            lines[index] = '{} = {}'.format(1, lines[index].split('=')[1])
        with open(self.params["in_folder"] + "parameter.dat", 'w') as file_obj:
            file_obj.writelines(lines)

    def run_dynust(self):
        executable = self.params["in_folder"] + "DynusTv3bx64_0801.exe"
        xxx = self.params["in_folder"]
        os.chdir(xxx)
        scenario = os.path.basename(xxx)
        xxx += ("\\" + scenario + ".dws")
        print("Running DynusT...\n", xxx, '\n', scenario)
        os.spawnv(os.P_WAIT, executable, [executable, xxx])

    def post_run(self):
        '''
        Process results from DynusT
        '''
        print("Copying files...")
        output_file = self.params["out_vehicle"]
        if os.path.exists(output_file):
            os.remove(output_file)
        input_file = self.params["in_folder"] + "output_vehicle.dat"
        try:
            copyfile(input_file, output_file)
        except SameFileError:
            print('Failed in preparing output_vehicle.dat!\nThe destination ' +
                  'file is the same as source.')
        except OSError:
            print('Failed in preparing output_vehicle.dat!\nYou do not have ' +
                  'write access for the destination file.')
        output_path = self.params["out_path"]
        if os.path.exists(output_path):
            os.remove(output_path)
        input_path = self.params["in_folder"]+"output_path.dat"
        try:
            copyfile(input_path, output_path)
        except SameFileError:
            print('Failed in preparing output_path.dat!\nThe destination ' +
                  'file is the same as source.')
        except OSError:
            print('Failed in preparing output file!\nYou do not have write ' +
                  'access for the destination file.')

    def travel_time(self):
        '''
        Read trajectories produced by DynusT, feed records to load() and store
        results in output file.
        '''
        input_file = self.params["in_folder"] + "vehtrajectory.itf"
        if not os.path.exists(input_file):
            print("Missing vehtrajectory.itf!!!")
            return
        with open(input_file, 'rb') as file_obj:
            while True:
                if not self.load(file_obj):
                    break
        print('Number of records: {}'.format(len(self.vehicle_id)))
        output_file = self.params["out_time"]
        with open(output_file, 'wb') as file_obj:
            data = struct.pack('i', len(self.vehicle_id))
            file_obj.write(data)
            veh_ids = array('i')
            veh_ids.fromlist(self.vehicle_id)
            tot_times = array('f')
            tot_times.fromlist(self.total_time)
            tags = array('i')
            tags.fromlist(self.tag)
            np.savez_compressed(
                file_obj,
                veh_ids=veh_ids,
                tot_times=tot_times,
                tags=tags)
            # veh_ids.tofile(file_obj)  # type: ignore
            # tot_times.tofile(file_obj)  # type: ignore
            # tags.tofile(file_obj)  # type: ignore

    def load(self, file_obj):
        '''
        Load records from output of DynusT and store vehicle ids, travel times,
        and tags.
        '''
        try:
            data = file_obj.read(self.header_info['size'])
            fields = struct.unpack(self.header_info['fmt'], data)
            vehicle_id, tag = fields[:2]
            total_time, nodes = fields[10:12]
            self.record += 1
            self.vehicle_id.append(vehicle_id)
            self.total_time.append(total_time)
            self.tag.append(tag)
        except struct.error:
            print('Reached end of records.')
            return False
        if not self.read_values_line(file_obj, int):
            return False
        if tag == 1 and nodes <= 1:
            return True
        else:
            for _ in range(3):
                if not self.read_values_line(file_obj, float):
                    return False
        return True

    def read_values_line(self, file_obj, dtype):
        '''
        Read a line of values. Either nodes, cum_time, step_time, or delay.
        '''
        try:
            data = file_obj.read(self.value_info['size'])
            fields = struct.unpack(self.value_info['fmt'], data)
            nodes, = fields
        except struct.error:
            print('No more nodes found.')
            return False
        try:
            _ = np.fromfile(file_obj, dtype=dtype, count=nodes)
        except ValueError as error:
            print(error)
            return False
        except IOError as error:
            print(error)
            return False
        return True

    def run(self):
        '''
        Run pipeline
        '''
        self.prepare_vehicle()
        self.prepare_path()
        self.prepare_dynust()
        self.run_dynust()
        self.post_run()
        self.travel_time()


# if __name__ == "__main__":
#     AB_DST2(sys.argv[1])
