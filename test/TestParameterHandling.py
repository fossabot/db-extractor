from datetime import datetime
from common.BasicNeeds import BasicNeeds
from db_extractor.ParameterHandling import ParameterHandling
# package to facilitate multiple operation system operations
import unittest


class MyTestCase(unittest.TestCase):

    def test_interpret_known_expression(self):
        bn = BasicNeeds()
        json_structure = bn.fn_open_file_and_get_content('expressions.json')
        pair_values = []
        index_counter = 0
        for current_expression_group in json_structure.items():
            for current_expression in current_expression_group[1]:
                pair_values.append(index_counter)
                pair_values[index_counter] = current_expression
                index_counter += 1
        ph = ParameterHandling()
        for current_pair in pair_values:
            reference_date = datetime.strptime(current_pair['reference_date'], '%Y-%m-%d')
            expression_parts = current_pair['expression'].split('_')
            value_to_assert = ph.interpret_known_expression(reference_date, expression_parts)
            self.assertEqual(value_to_assert, current_pair['expected'],
                             'Provided value was "' + current_pair['reference_date']
                             + '", Expression was "' + current_pair['expression'] + '" '
                             + '", Expected was "' + current_pair['expected'] + '" '
                             + 'but received was "' + value_to_assert + '"...')


if __name__ == '__main__':
    unittest.main()

