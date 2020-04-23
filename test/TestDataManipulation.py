from sources.common.DataManipulator import DataManipulator
from sources.common.FileOperations import FileOperations
# package to facilitate multiple operation system operations
import unittest


class TestDataManipulator(unittest.TestCase):

    def test_add_value_to_dictionary(self):
        fo = FileOperations()
        # load testing values from JSON file
        # where all cases are grouped
        json_structure = fo.fn_open_file_and_get_content('dict_to_test.json')
        c_dm = DataManipulator()
        for add_type in json_structure:
            if 'reference' not in json_structure[add_type]:
                json_structure[add_type]['reference'] = None
            given = c_dm.add_value_to_dictionary(json_structure[add_type]['list'],
                                                 json_structure[add_type]['add'], add_type,
                                                 json_structure[add_type]['reference'])
            self.assertEqual(given, json_structure[add_type]['expected'])
