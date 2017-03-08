import abc


class BasePropGen(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, template, parameters):
        self.template = template
        self.user_params = parameters
        self.params = self._get_params()
        self.root_path_keys = []
        self.loop_path_keys = []

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

    def _set_values(self, outer, inner):
        '''
        For non path values in the property file update the values to be those
        entered by the user.

        Args:
            outer (string): The current outer loop iteration number.
            inner (string): The current inner loop iteration number.
        '''
        for key, value in self.user_params.items():
            if key in self.root_path_keys or key in self.loop_path_keys:
                self._set_path(key, outer, inner)
            self.params[key] = value

    def _set_path(self, key, outer, inner):
        '''
        Set the proper paths for the values in the property file that need
        them.

        Args:
            outer (string): The current outer loop iteration number.
            inner (string): The current inner loop iteration number.
        '''
        if key in self.root_path_keys:
            self.set_root_paths()
        else:
            self.set_loop_paths(outer, inner)
            root = self.user_params['root'] + '\\'
            path = 'outer{}\\inner{}\\'.format(outer, inner)
            path = root + path + self.loop_path_keys[key].replace('#', inner)
            self.params[key] = path

    @abc.abstractmethod
    def set_root_paths(self):
        '''Method for setting the BASEPATH segment of values that do not
        contain loop folders in their path.'''

    @abc.abstractmethod
    def set_loop_paths(self, outer, inner):
        '''Method for setting the BASEPATH and loop directory segments of
        values that do contain loop folders in their path.'''

    def create(self, filename, outer, inner):
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


class DSTPropertyGenerator(BasePropGen):
    def __init__(self, template, parameters):
        super(DSTPropertyGenerator, self).__init__(template, parameters)
        self.root_path_keys =
