'''
process_manager.py: This module defines a class that can be used to execute
the ABM->ISAM->DTA pipeline.
'''

import subprocess
from ab_dst2 import AB_DST2


class ProcessManager(object):
    '''
    Class for managing the processes involved with iterating though ISAM-DTA
    loops.
    '''
    def __init__(self, isam_properties, dta_properties):
        self.isam_props = isam_properties
        self.dta_props = dta_properties

    def call_command(self, command):
        '''
        Call a shell command and wait for it to execute.
        Input:
            command: String. A command that executes on the command line.

        Example:
            call_command('runISAM.cmd', check=True)
        This will execute runISAM.cmd and wait for it to exit.
        '''
        subprocess.run(command, check=True)

    def run_dynust(self):
        '''
        Create instance of AB_DST2 with self.dta_properties and use it to run
        dynusT.
        '''
        ab_dst = AB_DST2(self.dta_props)
        ab_dst.run()
