import os
from sources.common.BasicNeeds import BasicNeeds
# package to facilitate multiple operation system operations
import unittest


class TestBasicNeeds(unittest.TestCase):

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
