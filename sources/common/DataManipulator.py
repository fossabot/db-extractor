"""
Data Manipulation class
"""
# package to handle date and times
from datetime import timedelta
# package facilitating Data Frames manipulation
import pandas as pd


class DataManipulator:

    @staticmethod
    def fn_add_days_within_column_to_data_frame(input_data_frame, dict_expression):
        input_data_frame['Days Within'] = input_data_frame[dict_expression['End Date']] - \
                                          input_data_frame[dict_expression['Start Date']] + \
                                          timedelta(days=1)
        input_data_frame['Days Within'] = input_data_frame['Days Within'] \
            .apply(lambda x: int(str(x).replace(' days 00:00:00', '')))
        return input_data_frame

    @staticmethod
    def fn_add_minimum_and_maximum_columns_to_data_frame(input_data_frame, dict_expression):
        grouped_df = input_data_frame.groupby(dict_expression['group_by']) \
            .agg({dict_expression['calculation']: ['min', 'max']})
        grouped_df.columns = ['_'.join(col).strip() for col in grouped_df.columns.values]
        grouped_df = grouped_df.reset_index()
        if 'map' in dict_expression:
            grouped_df.rename(columns=dict_expression['map'], inplace=True)
        return grouped_df

    def fn_add_timeline_evaluation_column_to_data_frame(self, in_df, dict_expression):
        # shorten last method parameter
        de = dict_expression
        # add helpful column to use on "Timeline Evaluation" column determination
        in_df['rd'] = de['Reference Date']
        # shorten remaining columns to use on "Timeline Evaluation" column determination
        se_map = {
            de['Start Date']: 's',
            de['End Date']: 'e'
        }
        in_df.rename(columns=se_map, inplace=True)
        # variable to pick relevant columns for "Timeline Evaluation" column determination
        cols = ['rd', 's', 'e']
        # actual "Timeline Evaluation" column determination
        in_df['Timeline Evaluation'] = in_df[cols] \
            .apply(lambda r: 'Current' if r['s'] <= r['rd'] <= r['e'] else 'Past'\
                   if r['s'] < r['rd'] else 'Future', axis=1)
        # build dictionary to use for original column names restoration
        restore_map = {
            's': de['Start Date'],
            'e': de['End Date'],
        }
        # decide if the helpful column is to be retained or not
        removal_needed = self.fn_decide_by_omission_or_specific_false(dict_expression,
                                                                      'Reference Date Retention')
        if removal_needed:
            in_df.drop(columns=['rd'], inplace=True)
        else:
            restore_map['rd'] = 'Reference Date'
        # original column names restoration
        in_df.rename(columns=restore_map, inplace=True)
        return in_df

    @staticmethod
    def fn_add_weekday_columns_to_data_frame(input_data_frame, columns_list):
        for current_column in columns_list:
            input_data_frame['Weekday for ' + current_column] = input_data_frame[current_column] \
                .apply(lambda x: x.strftime('%A'))
        return input_data_frame

    @staticmethod
    def fn_apply_query_to_data_frame(local_logger, timmer, input_data_frame, extract_params):
        timmer.start()
        query_expression = ''
        if extract_params['filter_to_apply'] == 'equal':
            local_logger.debug('Will retain only values equal with "'
                               + extract_params['filter_values'] + '" within the field "'
                               + extract_params['column_to_filter'] + '"')
            query_expression = '`' + extract_params['column_to_filter'] + '` == "' \
                               + extract_params['filter_values'] + '"'
        elif extract_params['filter_to_apply'] == 'different':
            local_logger.debug('Will retain only values different than "'
                               + extract_params['filter_values'] + '" within the field "'
                               + extract_params['column_to_filter'] + '"')
            query_expression = '`' + extract_params['column_to_filter'] + '` != "' \
                               + extract_params['filter_values'] + '"'
        elif extract_params['filter_to_apply'] == 'multiple_match':
            local_logger.debug('Will retain only values equal with "'
                               + extract_params['filter_values'] + '" within the field "'
                               + extract_params['column_to_filter'] + '"')
            query_expression = '`' + extract_params['column_to_filter'] + '` in ["' \
                               + '", "'.join(extract_params['filter_values'].values()) \
                               + '"]'
        local_logger.debug('Query expression to apply is: ' + query_expression)
        input_data_frame.query(query_expression, inplace=True)
        timmer.stop()
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
            input_data_frame[current_column] = pd.to_datetime(input_data_frame[current_column],
                                                              format=columns_format)
        return input_data_frame

    @staticmethod
    def fn_decide_by_omission_or_specific_false(in_dictionary, key_decision_factor):
        removal_needed = False
        if key_decision_factor not in in_dictionary:
            removal_needed = True
        elif not in_dictionary[key_decision_factor]:
            removal_needed = True
        return removal_needed

    def fn_drop_certain_columns(self, local_logger, timmer, working_dictionary):
        for current_file in working_dictionary['files']:
            # load all relevant files into a single data frame
            df = self.fn_load_file_list_to_data_frame(local_logger, timmer, [current_file],
                                                      working_dictionary['csv_field_separator'])
            save_necessary = False
            for column_to_eliminate in working_dictionary['columns_to_eliminate']:
                if column_to_eliminate in df:
                    df.drop(columns=column_to_eliminate, inplace=True)
                    save_necessary = True
            if save_necessary:
                self.fn_store_data_frame_to_file(local_logger, timmer, df, current_file,
                                                 working_dictionary['csv_field_separator'])

    @staticmethod
    def fn_filter_data_frame_by_index(local_logger, in_data_frame, filter_rule):
        index_current = in_data_frame.query('`Timeline Evaluation` == "Current"', inplace=False)
        local_logger.info('Current index has been determined to be ' + str(index_current.index))
        if 'Deviation' in filter_rule:
            for deviation_type in filter_rule['Deviation']:
                deviation_number = filter_rule['Deviation'][deviation_type]
                if deviation_type == 'Lower':
                    index_to_apply = index_current.index - deviation_number
                    in_data_frame = in_data_frame[in_data_frame.index >= index_to_apply[0]]
                elif deviation_type == 'Upper':
                    index_to_apply = index_current.index + deviation_number
                    in_data_frame = in_data_frame[in_data_frame.index <= index_to_apply[0]]
                local_logger.info(deviation_type + ' Deviation Number is ' + str(deviation_number)
                                  + ' to be applied to Current index, became '
                                  + str(index_to_apply))
        return in_data_frame

    @staticmethod
    def fn_load_file_list_to_data_frame(local_logger, timmer, file_list, csv_delimiter):
        timmer.start()
        combined_csv = pd.concat([pd.read_csv(filepath_or_buffer=current_file,
                                              delimiter=csv_delimiter,
                                              cache_dates=True,
                                              index_col=None,
                                              memory_map=True,
                                              low_memory=False,
                                              encoding='utf-8',
                                              ) for current_file in file_list])
        local_logger.info('All relevant files were merged into a Pandas Data Frame')
        timmer.stop()
        return combined_csv

    @staticmethod
    def fn_store_data_frame_to_file(local_logger, timmer, input_data_frame, input_file_details):
        timmer.start()
        if input_file_details['format'] == 'csv':
            input_data_frame.to_csv(path_or_buf=input_file_details['name'],
                                    sep=input_file_details['field-delimiter'],
                                    header=True,
                                    index=False,
                                    encoding='utf-8')
        local_logger.info('Data frame has just been saved to file "'
                          + input_file_details['name'] + '"')
        timmer.stop()
