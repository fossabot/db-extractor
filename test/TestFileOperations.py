import os
from sources.common.FileOperations import FileOperations
# package to facilitate multiple operation system operations
import platform
import unittest


class TestFileOperations(unittest.TestCase):

    def test_file_statistics(self):
        parent_folder = os.path.abspath(os.pardir) + '/' + '/'.join([
            'virtual_environment',
            'Scripts'
        ])
        relevant_file = 'pip'
        if platform.system() == 'Windows':
            relevant_file += '.exe'
        file_to_evaluate = os.path.join(parent_folder, relevant_file)
        fo = FileOperations()
        value_to_assert = fo.fn_get_file_statistics(file_to_evaluate)['size [bytes]']
        value_to_compare_with = os.path.getsize(file_to_evaluate)
        self.assertEqual(value_to_assert, value_to_compare_with)
