'''
process_manager.py: This module defines a class that can be used to execute
the ABM->ISAM->DTA pipeline.
'''

import subprocess
from os import makedirs, path
from shutil import copy2
from ab_dst2 import AB_DST2


class ProcessManager(object):
    '''
    Class for managing the processes involved with iterating though ISAM-DTA
    loops.
    '''
    def __init__(self):
        self.commands = ['runIsam.cmd', 'runVehicleWriter.cmd']
        self.sumary_stats = [
            'Convergence.dat', 'OutMUC.dat', 'SummaryStat.dat']
        self.iter = 0

    def call_command(self, script):
        '''
        Call a shell command and wait for it to execute.

        Args:
            script (string): A command that executes on the command line.

        Example:
            >>> call_command('runISAM.cmd', (0))
            Execute runISAM.cmd with the argument 0 and wait for the
        process to exit.
        '''
        command = script + ' ' + str(self.iter)
        result = subprocess.run(
            command,
            check=True,
            stderr=subprocess.PIPE,
            universal_newlines=True)
        event_file = 'iter{0}\\event_{1}{2}{0}.log'.format(
            self.iter, command[3].lower(), command[4:-4])
        self._copy_log_file('event.log', event_file)

    def run_dynust(self):
        '''
        Create instance of AB_DST2 with self.dta_properties and use it to run
        dynusT.
        '''
        prop_file = 'ab_dst_{}.dat'.format(self.iter)
        ab_dst = AB_DST2(prop_file)
        ab_dst.run()

    @staticmethod
    def _copy_file(source, destination):
        '''
        Private static method for copying and saving results from each
        iteration.

        Args:
            source (string): Path to the source file.
            destination (string): Path of where to copy source to.
        '''
        directory = path.dirname(destination)
        if not path.exists(directory):
            makedirs(directory)
        copy2(source, destination)

    def run_iteration(self):
        '''
        Run single iteration of inner loop. ISAM -> VehWriter -> AB_DST.
        '''
        for command in self.commands:
            self.call_command(command)
        self.run_dynust()
        loop_dir = 'inner{}\\'.format(self.iter)
        for summary in self.sumary_stats:
            dest = loop_dir + summary[:-4] + '{}.dat'.format(self.iter)
            self._copy_log_file(summary, dest)
