"""
Testing key methods from Parameter Handling class
"""
from datetime import datetime
from sources.common.FileOperations import os, FileOperations
from sources.db_extractor.ParameterHandling import ParameterHandling
import unittest


class TestParameterHandling(unittest.TestCase):

    def test_interpret_known_expression(self):
        fo = FileOperations()
        # load testing values from JSON file
        # where all cases are grouped
        json_structure = fo.fn_open_file_and_get_content(os.path.join(os.path.dirname(__file__), 'expressions.json'))
        # flatten out all testing values
        pair_values = []
        index_counter = 0
        for current_expression_group in json_structure.items():
            for current_expression in current_expression_group[1]:
                pair_values.append(index_counter)
                pair_values[index_counter] = current_expression
                index_counter += 1
        # parse through all pair of values and run the test
        ph = ParameterHandling()
        for current_pair in pair_values:
            reference_format = '%Y-%m-%d'
            if 'reference_format' in current_pair:
                reference_format = current_pair['reference_format']
            reference_date = datetime.strptime(current_pair['reference_value'], reference_format)
            expression_parts = current_pair['expression'].split('_')
            value_to_assert = ph.interpret_known_expression(reference_date, expression_parts, 1)
            self.assertEqual(value_to_assert, current_pair['expected_value'],
                             'Provided value was "' + current_pair['reference_value']
                             + '", Expression was "' + current_pair['expression'] + '" '
                             + '", Expected was "' + current_pair['expected_value'] + '" '
                             + 'but received was "' + value_to_assert + '"...')


if __name__ == '__main__':
    unittest.main()

