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
        self.inner = 0
        self.outer = 0

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
        command = script + ' ' + str(self.inner)
        result = subprocess.run(
            command,
            check=True,
            stderr=subprocess.PIPE,
            universal_newlines=True)
        print('Ran command: {}'.format(command))
        event_file = 'outer{0}\\inner{1}\\event_{2}{3}{1}.log'.format(
            self.outer, self.inner, command[3].lower(), command[4:-4])
        self._copy_file('event.log', event_file)

    def run_dynust(self):
        '''
        Create instance of AB_DST2 with self.dta_properties and use it to run
        dynusT.
        '''
        prop_file = 'outer{}\\ab_dst_{}.dat'.format(
            self.outer, self.inner)
        ab_dst = AB_DST2(prop_file)
        print('AB_DST2 created with property file: {}'.format(prop_file))
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

    def _run_inner(self):
        '''
        Run single iteration of inner loop. ISAM -> VehWriter -> AB_DST.
        '''
        for command in self.commands:
            self.call_command(command)
        self.run_dynust()
        loop_dir = 'outer{}\\inner{}\\'.format(self.outer, self.inner)
        for summary in self.sumary_stats:
            dest = loop_dir + summary[:-4] + '{}.dat'.format(self.inner)
            src = 'dynust\\' + summary
            self._copy_file(src, dest)

    def _run_outer(self):
        '''
        Run single iteration of outer loop. This involves 8 iterations of
        run_inner.
        '''
        for _ in range(9):
            self._run_inner()
            self.inner += 1

    def run(self):
        '''
        Run complete iteration process. 3 outer loops containing 8 inner loops
        each.
        '''
        for _ in range(3):
            self._run_outer()
            self.outer += 1
