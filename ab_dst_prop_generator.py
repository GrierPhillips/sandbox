'''
ab_dst_prop_generator.py: This module defines a class that can be used to
generate property files for use by ab_dst2.py
'''


class ABDSTPropertyGenerator(object):
    '''
    Class used for accepting parameters from a user and modifying a tempalte
    property file with the updated values.
    '''
    def __init__(self, template, parameters):
        self.template = template
        self.user_params = parameters
        self.params = self._get_params()
        self.path_params = {
            'in_folder': None,
            'in_vehicle': 'abm_vehicle_#.dat',
            'out_vehicle': 'output_vehicle_#.dat',
            'out_path': 'output_path_#.dat',
            'out_time': 'output_time_#.bin'}

    def _open_template(self):
        '''
        Open template file and return a list of lines.
        '''
        with open(self.template, 'r') as template:
            lines = template.readlines()
        return lines

    def _get_params(self):
        '''
        Create a dictionary of pramaters: values from lines in template.
        '''
        lines = [line.strip().split('=') for line in self._open_template()]
        params = dict(line for line in lines)
        return params

    def _set_path(self, key, outer, inner):
        '''
        Set the proper paths for the values in the property file that need
        them.

        Args:
            outer (string): The current outer loop iteration number.
            inner (string): The current inner loop iteration number.
        '''
        if key != 'in_folder':
            self.params[key] = self.user_params['root'] + '\\dynust\\'
        else:
            root = self.user_params['root'] + '\\'
            path = 'outer{}\\inner{}\\'.format(outer, inner)
            path = root + path + self.path_params[key].replace('#', inner)
            self.params[key] = path

    def set_values(self, outer, inner):
        '''
        For non path values in the property file update the values to be those
        entered by the user.

        Args:
            outer (string): The current outer loop iteration number.
            inner (string): The current inner loop iteration number.
        '''
        for key, value in self.user_params.items():
            if key in self.path_params.keys():
                self._set_path(key, outer, inner)
            self.params[key] = value

    def write(self, filename, outer, inner):
        '''
        Construct the parameter file and write it to the desired location.

        Args:
            filename (string): Path from root containing name of where file
            will be saved.
            outer (string): The current outer loop iteration number.
            inner (string): The current inner loop iteration number.
        '''
        self.set_values(outer, inner)
        lines = []
        for item in self.params.items():
            lines.append('='.join(item) + '\n')
        with open(filename, 'w') as file_obj:
            file_obj.writelines(lines)
