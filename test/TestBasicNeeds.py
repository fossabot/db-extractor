from db_extractor.BasicNeeds import BasicNeeds
from db_extractor.FileOperations import FileOperations
# package to facilitate multiple operation system operations
import unittest


class TestBasicNeeds(unittest.TestCase):

    def test_add_value_to_dictionary(self):
        class_fo = FileOperations()
        # load testing values from JSON file where all cases are grouped
        json_structure = class_fo.fn_open_file_and_get_content('dict_to_test.json')
        class_bn = BasicNeeds()
        for add_type in json_structure:
            if 'reference' not in json_structure[add_type]:
                json_structure[add_type]['reference'] = None
            actual = class_bn.fn_add_value_to_dictionary(json_structure[add_type]['list'].copy(),
                                                         json_structure[add_type]['add'], add_type,
                                                         json_structure[add_type]['reference'])
            self.assertEqual(json_structure[add_type]['expected'].copy(), actual, 'With '
                             + 'value "' + json_structure[add_type]['add'] + '" to be added "'
                             + add_type + '" the "' + str(json_structure[add_type]['reference'])
                             + '" in the list ' + str(json_structure[add_type]['list'].copy())
                             + ' expected values were: '
                             + str(json_structure[add_type]['expected'].copy())
                             + ' but found ' + str(actual))

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
