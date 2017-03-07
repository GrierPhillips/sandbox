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
    def __init__(self, dta_properties):
        self.dta_props = dta_properties

    def call_command(self, script, iteration):
        '''
        Call a shell command and wait for it to execute.
        Input:
            command: String. A command that executes on the command line.
            args: Iterator. A collection of command line arguments.
        Example:
            call_command('runISAM.cmd', (0))
        This will execute runISAM.cmd with the argument 0 and wait for the
        process to exit.
        '''
        command = script + ' ' + str(iteration)
        result = subprocess.run(
            command,
            check=True,
            stderr=subprocess.PIPE,
            universal_newlines=True)
        event_file = 'iter{2}\\event_{0}{1}{2}.log'.format(
            command[3].lower(), command[4:-4], iteration)
        self._copy_log_file('event.log', event_file)

    def run_dynust(self):
        '''
        Create instance of AB_DST2 with self.dta_properties and use it to run
        dynusT.
        '''
        ab_dst = AB_DST2(self.dta_props)
        ab_dst.run()

    @staticmethod
    def _copy_log_file(source, destination):
        directory = path.dirname(destination)
        if not path.exists(directory):
            makedirs(directory)
        copy2(source, destination)
