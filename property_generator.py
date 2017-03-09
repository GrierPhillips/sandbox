'''
property_generator.py: This module contains the BasePropGen class. This class
can be used to generate property files for the AB_DST class and for use with
the runIsam.cmd batch file.
'''
import abc


class BasePropGen(object):
    '''
    Abstract base class for defining property generator classes.
    '''
    __metaclass__ = abc.ABCMeta

    def __init__(self, template, parameters):
        self.template = template
        self.user_params = parameters
        self.params = self._get_params()
        self.root_path_keys = []
        self.loop_path_keys = []
        self._get_path_keys()

    def _read_temp(self):
        '''
        Open template file and return a list of lines.
        '''
        with open(self.template, 'r', encoding='cp1252') as template:
            lines = template.readlines()
        return lines

    def _get_params(self):
        '''
        Create a dictionary of pramaters: values from lines in template.
        '''
        lines = [line.strip().split('=') for line in self._read_temp()]
        lines = [line for line in lines if len(line) != 1]
        lines = [line for line in lines if not line[0].startswith('#')]
        params = dict(line for line in lines)
        for key, value in params.items():
            params[key] = value.strip()
        return params

    def _get_path_keys(self):
        for key, value in self.params.items():
            if 'BASEPATH' in value:
                if 'LOOP_PAIR' in value or 'OUTER' in value:
                    self.loop_path_keys.append(key)
                else:
                    self.root_path_keys.append(key)

    def _set_values(self, outer, inner):
        '''
        For non path values in the property file update the values to be those
        entered by the user.

        Args:
            outer (string): The current outer loop iteration number.
            inner (string): The current inner loop iteration number.
        '''
        basepath = self.user_params['basepath']
        for key, value in self.user_params.items():
            if key in self.root_path_keys or key in self.loop_path_keys:
                self._set_path(key, outer, inner, basepath)
            self.params[key] = value

    def _set_path(self, key, outer, inner, basepath):
        '''
        Set the proper paths for the values in the property file that need
        them.

        Args:
            outer (string): The current outer loop iteration number.
            inner (string): The current inner loop iteration number.
            basepath (string): The value entered by user for BASEPATH.
        '''
        self.set_root_paths(key, basepath)
        if key in self.loop_path_keys:
            self.set_loop_paths(key, outer, inner)

    def set_root_paths(self, key, basepath):
        '''
        Set the BASEPATH value in the parameter for key to the value entered
        by the user.

        Args:
            key (string): The key for the parameter to be set.
            basepath (string): The value entered by user for BASEPATH.
        '''
        self.params[key] = self.params[key].replace('BASEPATH', basepath)

    def set_loop_paths(self, key, outer, inner):
        '''
        Set the LOOP_PAIR value in the parameter for key to the value of the
        current outer loop and inner loop.

        Args:
            key (string): The key for the parameter to be set.
            outer (string): The current outer loop iteration number.
            inner (string): The current inner loop iteration number.
        '''
        if 'LOOP_PAIR' in self.params[key]:
            loop_pair = 'outer{}\\inner{}'.format(outer, inner)
            self.params[key] = self.params[key].replace('LOOP_PAIR', loop_pair)
        else:
            outer_loop = 'outer{}'.format(outer)
            self.params[key] = self.params[key].replace('OUTER', outer_loop)

    def create(self, filename, outer, inner):
        '''
        Construct the parameter file and write it to the desired location.

        Args:
            filename (string): Path from root containing name of where file
            will be saved.
            outer (string): The current outer loop iteration number.
            inner (string): The current inner loop iteration number.
        '''
        self._set_values(outer, inner)
        lines = []
        for item in self.params.items():
            lines.append('='.join(item) + '\n')
        with open(filename, 'w') as file_obj:
            file_obj.writelines(lines)
