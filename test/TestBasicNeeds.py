import os
from sources.db_extractor.BasicNeeds import BasicNeeds
# package to facilitate multiple operation system operations
import platform
import unittest


class MyTestCase(unittest.TestCase):

    def test_file_statistics(self):
        parent_folder = os.path.abspath(os.pardir) + '/' + '/'.join([
            'virtual_environment',
            'Scripts'
        ])
        relevant_file = 'pip'
        if platform.system() == 'Windows':
            relevant_file += '.exe'
        file_to_evaluate = os.path.join(parent_folder, relevant_file)
        bn = BasicNeeds()
        value_to_assert = bn.fn_get_file_statistics(file_to_evaluate)['size [bytes]']
        value_to_compare_with = os.path.getsize(file_to_evaluate)
        self.assertEqual(value_to_assert, value_to_compare_with)

    def test_numbers_with_leading_zero(self):
        pair_values = [
            {
                'given': '1',
                'leading_zeros': 2,
                'expected': '01'
            },
            {
                'given': '1',
                'leading_zeros': 10,
                'expected': '0000000001'
            },
            {
                'given': '1',
                'leading_zeros': 1,
                'expected': '1'
            }
        ]
        bn = BasicNeeds()
        for current_pair in pair_values:
            
            value_to_assert = bn.fn_numbers_with_leading_zero(current_pair['given'],
                                                              current_pair['leading_zeros'])
            self.assertEqual(value_to_assert, current_pair['expected'],
                             'Provided values was "' + value_to_assert + '", Expected')
        pair_failing_values = [
            {
                'given': '1',
                'leading_zeros': 2,
                'expected': '02'
            },
            {
                'given': '1',
                'leading_zeros': 0,
                'expected': ''
            }
        ]
        for current_pair in pair_failing_values:
            value_to_assert = bn.fn_numbers_with_leading_zero(current_pair['given'],
                                                              current_pair['leading_zeros'])
            self.assertNotEqual(value_to_assert, current_pair['expected'])


if __name__ == '__main__':
    unittest.main()
