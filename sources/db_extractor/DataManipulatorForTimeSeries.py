"""
Data Manipulation for Time Series class
"""
# package to handle date and times
from datetime import timedelta
# package facilitating Data Frames manipulation
import pandas
# custom classes
from db_extractor.BasicNeeds import BasicNeeds


class DataManipulatorForTimeSeries:
    class_bn = None

    def __init__(self, in_language='en_US'):
        self.class_bn = BasicNeeds(in_language)

    @staticmethod
    def fn_add_days_within_column_to_data_frame(input_data_frame, dict_expression):
        input_data_frame['Days Within'] = input_data_frame[dict_expression['End Date']] - \
                                          input_data_frame[dict_expression['Start Date']] + \
                                          timedelta(days=1)
        input_data_frame['Days Within'] = input_data_frame['Days Within'] \
            .apply(lambda x: int(str(x).replace(' days 00:00:00', '')))
        return input_data_frame

    def fn_add_timeline_evaluation_column_to_data_frame(self, in_df, dict_expression):
        # shorten last method parameter
        de = dict_expression
        # add helpful column to use on "Timeline Evaluation" column determination
        in_df['rd'] = de['Reference Date']
        # rename some columns to cope with long expression
        in_df.rename(columns={'Start Date': 'sd', 'End Date': 'ed'}, inplace=True)
        # actual "Timeline Evaluation" column determination
        cols = ['rd', 'sd', 'ed']
        in_df['Timeline Evaluation'] = in_df[cols].apply(lambda r: 'Current'
                                                         if r['sd'] <= r['rd'] <= r['ed'] else
                                                         'Past' if r['sd'] < r['rd'] else 'Future',
                                                         axis=1)
        # rename back columns
        in_df.rename(columns={'sd': 'Start Date', 'ed': 'End Date', 'rd': 'Reference Date'},
                     inplace=True)
        # decide if the helpful column is to be retained or not
        removal_needed = self.class_bn.fn_decide_by_omission_or_specific_true(
            de, 'Remove Reference Date')
        if removal_needed:
            in_df.drop(columns=['Reference Date'], inplace=True)
        return in_df

    @staticmethod
    def fn_add_weekday_columns_to_data_frame(input_data_frame, columns_list):
        for current_column in columns_list:
            input_data_frame['Weekday for ' + current_column] = input_data_frame[current_column] \
                .apply(lambda x: x.strftime('%A'))
        return input_data_frame

    @staticmethod
    def fn_convert_datetime_columns_to_string(input_data_frame, columns_list, columns_format):
        for current_column in columns_list:
            input_data_frame[current_column] = \
                input_data_frame[current_column].map(lambda x: x.strftime(columns_format))
        return input_data_frame

    @staticmethod
    def fn_convert_string_columns_to_datetime(input_data_frame, columns_list, columns_format):
        for current_column in columns_list:
            input_data_frame[current_column] = pandas.to_datetime(input_data_frame[current_column],
                                                                  format=columns_format)
        return input_data_frame

    @staticmethod
    def fn_get_first_current_and_last_column_value_from_data_frame(in_data_frame, in_column_name):
        return {
            'first': in_data_frame.iloc[0][in_column_name],
            'current': in_data_frame.query('`Timeline Evaluation` == "Current"',
                                           inplace=False)[in_column_name].max(),
            'last': in_data_frame.iloc[(len(in_data_frame) - 1)][in_column_name],
        }
