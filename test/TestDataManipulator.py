# useful methods to measure time performance by small pieces of code
from codetiming import Timer
# package facilitating Data Frames manipulation
import pandas
from db_extractor.FileOperations import FileOperations
from db_extractor.LoggingNeeds import LoggingNeeds
from db_extractor.DataManipulator import DataManipulator
import unittest
import re


class TestDataManipulator(unittest.TestCase):

    def test_add_and_shift_column(self):
        class_ln = LoggingNeeds()
        class_ln.initiate_logger('None', __file__)
        timer = Timer(__file__, text='Time spent is {seconds}', logger=class_ln.logger.debug)
        class_fo = FileOperations()
        # load testing values from JSON file where all cases are grouped
        json_structure = class_fo.fn_open_file_and_get_content('series.json')
        for crt_series in json_structure:
            in_data_frame = pandas.DataFrame(crt_series['Given'])
            class_dm = DataManipulator()
            out_data_frame = class_dm.fn_add_and_shift_column(class_ln.logger, timer, in_data_frame, [
                crt_series['Parameters']
            ])
            out_data_frame[['Column_New']] = out_data_frame[['Column_New']]\
                .apply(lambda x: int(x) if re.match('^[+-]*[0-9]+(\.{1}0*)*$', str(x)) else x)
            expected_data_frame = pandas.DataFrame(crt_series['Expected'])
            self.assertDictEqual(out_data_frame.to_dict(), expected_data_frame.to_dict())
