"""
Data Manipulation class
"""
# package to handle date and times
from datetime import timedelta
# package to add support for multi-language (i18n)
import gettext
# package to handle files/folders and related metadata/operations
import os
# package facilitating Data Frames manipulation
import pandas as pd


class DataManipulator:
    lcl = None

    def __init__(self, default_language='en_US'):
        current_script = os.path.basename(__file__).replace('.py', '')
        lang_folder = os.path.join(os.path.dirname(__file__), current_script + '_Locale')
        self.lcl = gettext.translation(current_script, lang_folder, languages=[default_language])

    def fn_add_and_shift_column(self, local_logger, timmer, input_data_frame, input_details):
        for in_dt in input_details:
            timmer.start()
            col_offset = -1 * in_dt['Deviation']
            if in_dt['Direction'] == 'down':
                col_offset = 1 * in_dt['Deviation']
            input_data_frame[in_dt['New Column']] = input_data_frame[in_dt['Original Column']]
            input_data_frame[in_dt['New Column']] = input_data_frame[in_dt['New Column']]\
                .shift(col_offset)
            fill_value = in_dt['Empty Values Replacement']
            input_data_frame[in_dt['New Column']] = input_data_frame[in_dt['New Column']]\
                .apply(lambda x: str(x).replace('.0', ''))\
                .apply(lambda x: str(x).replace('nan', in_dt['Empty Values Replacement']))
            local_logger.info(self.lcl.gettext(
                'A new column named "{new_column_name}" as copy from "{original_column}" '
                + 'then shifted by {shifting_rows} to relevant data frame '
                + '(filling any empty value as {empty_values_replacement})')
                              .replace('{new_column_name}', in_dt['New Column'])
                              .replace('{original_column}', in_dt['Original Column'])
                              .replace('{shifting_rows}', str(col_offset))
                              .replace('{empty_values_replacement}', fill_value))
            timmer.stop()
        return input_data_frame

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
        removal_needed = self.fn_decide_by_omission_or_specific_false(de, 'Keep Reference Date')
        if removal_needed:
            in_df.drop(columns=['Reference Date'], inplace=True)
        return in_df

    def fn_add_value_to_dictionary(self, in_list, adding_value, adding_type, reference_column):
        add_type = adding_type.lower()
        total_columns = len(in_list)
        reference_indexes = {
            'add': {'after': 0, 'before': 0},
            'cycle_down_to': {'after': 0, 'before': 0}
        }
        if type(reference_column) is int:
            reference_indexes = {
                'add': {
                    'after': in_list.copy().index(reference_column) + 1,
                    'before': in_list.copy().index(reference_column),
                },
                'cycle_down_to': {
                    'after': in_list.copy().index(reference_column),
                    'before': in_list.copy().index(reference_column),
                }
            }
        positions = {
            'after': {
                'cycle_down_to': reference_indexes.get('cycle_down_to').get('after'),
                'add': reference_indexes.get('add').get('after'),
            },
            'before': {
                'cycle_down_to': reference_indexes.get('cycle_down_to').get('before'),
                'add': reference_indexes.get('add').get('before'),
            },
            'first': {
                'cycle_down_to': 0,
                'add': 0,
            },
            'last': {
                'cycle_down_to': total_columns,
                'add': total_columns,
            }
        }
        return self.add_value_to_dictionary_by_position({
            'adding_value': adding_value,
            'list': in_list,
            'position_to_add': positions.get(add_type).get('add'),
            'position_to_cycle_down_to': positions.get(add_type).get('cycle_down_to'),
            'total_columns': total_columns,
        })

    @staticmethod
    def add_value_to_dictionary_by_position(adding_dictionary):
        list_with_values = adding_dictionary['list']
        list_with_values.append(adding_dictionary['total_columns'])
        for counter in range(adding_dictionary['total_columns'],
                             adding_dictionary['position_to_cycle_down_to'], -1):
            list_with_values[counter] = list_with_values[(counter - 1)]
        list_with_values[adding_dictionary['position_to_add']] = adding_dictionary['adding_value']
        return list_with_values

    @staticmethod
    def fn_add_weekday_columns_to_data_frame(input_data_frame, columns_list):
        for current_column in columns_list:
            input_data_frame['Weekday for ' + current_column] = input_data_frame[current_column] \
                .apply(lambda x: x.strftime('%A'))
        return input_data_frame

    def fn_apply_query_to_data_frame(self, local_logger, timmer, input_data_frame, extract_params):
        timmer.start()
        query_expression = ''
        generic_pre_feedback = self.lcl.gettext('Will retain only values {filter_type} '
                                                + '"{filter_values}" within the field '
                                                + '"{column_to_filter}"') \
            .replace('{column_to_filter}', extract_params['column_to_filter'])
        if extract_params['filter_to_apply'] == 'equal':
            local_logger.debug(generic_pre_feedback
                               .replace('{filter_type}', self.lcl.gettext('equal with'))
                               .replace('{filter_values}', extract_params['filter_values']))
            query_expression = '`' + extract_params['column_to_filter'] + '` == "' \
                               + extract_params['filter_values'] + '"'
        elif extract_params['filter_to_apply'] == 'different':
            local_logger.debug(generic_pre_feedback
                               .replace('{filter_type}', self.lcl.gettext('different than'))
                               .replace('{filter_values}', extract_params['filter_values']))
            query_expression = '`' + extract_params['column_to_filter'] + '` != "' \
                               + extract_params['filter_values'] + '"'
        elif extract_params['filter_to_apply'] == 'multiple_match':
            multiple_values = '["' + '", "'.join(extract_params['filter_values'].values()) + '"]'
            local_logger.debug(generic_pre_feedback
                               .replace('{filter_type}',
                                        self.lcl.gettext('matching any of these values'))
                               .replace('{filter_values}', multiple_values))
            query_expression = '`' + extract_params['column_to_filter'] + '` in ' + multiple_values
        local_logger.debug(self.lcl.gettext('Query expression to apply is: {query_expression}')
                           .replace('{query_expression}', query_expression))
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

    def fn_filter_data_frame_by_index(self, local_logger, in_data_frame, filter_rule):
        reference_expression = filter_rule['Query Expression for Reference Index']
        index_current = in_data_frame.query(reference_expression, inplace=False)
        local_logger.info(self.lcl.gettext(
            'Current index has been determined to be {index_current_value}')
                          .replace('{index_current_value}', str(index_current.index)))
        if str(index_current.index) != "Int64Index([], dtype='int64')" \
                and 'Deviation' in filter_rule:
            for deviation_type in filter_rule['Deviation']:
                deviation_number = filter_rule['Deviation'][deviation_type]
                index_to_apply = index_current.index
                if deviation_type == 'Lower':
                    index_to_apply = index_current.index - deviation_number
                    in_data_frame = in_data_frame[in_data_frame.index >= index_to_apply[0]]
                elif deviation_type == 'Upper':
                    index_to_apply = index_current.index + deviation_number
                    in_data_frame = in_data_frame[in_data_frame.index <= index_to_apply[0]]
                local_logger.info(self.lcl.gettext(
                    '{deviation_type} Deviation Number is {deviation_number} '
                    + 'to be applied to Current index, became {index_to_apply}')
                                  .replace('{deviation_type}', deviation_type)
                                  .replace('{deviation_number}', str(deviation_number))
                                  .replace('{index_to_apply}', str(index_to_apply)))
        return in_data_frame

    @staticmethod
    def fn_get_column_index_from_dataframe(data_frame_columns, column_name_to_identify):
        column_index_to_return = 0
        for ndx, column_name in enumerate(data_frame_columns):
            if column_name == column_name_to_identify:
                column_index_to_return = ndx
        return column_index_to_return

    @staticmethod
    def fn_get_first_current_and_last_column_value_from_data_frame(in_data_frame, in_column_name):
        return {
            'first': in_data_frame.iloc[0][in_column_name],
            'current': in_data_frame.query('`Timeline Evaluation` == "Current"',
                                           inplace=False)[in_column_name].max(),
            'last': in_data_frame.iloc[(len(in_data_frame) - 1)][in_column_name],
        }

    def fn_load_file_list_to_data_frame(self, local_logger, timmer, file_list, csv_delimiter):
        timmer.start()
        combined_csv = pd.concat([pd.read_csv(filepath_or_buffer=current_file,
                                              delimiter=csv_delimiter,
                                              cache_dates=True,
                                              index_col=None,
                                              memory_map=True,
                                              low_memory=False,
                                              encoding='utf-8',
                                              ) for current_file in file_list])
        local_logger.info(self.lcl.gettext(
            'All relevant files ({files_counted}) were merged into a Pandas Data Frame')
                          .replace('{files_counted}', str(len(file_list))))
        timmer.stop()
        return combined_csv

    @staticmethod
    def fn_save_data_frame_to_csv(in_data_frame, in_file_details):
        if 'field-delimiter' not in in_file_details:
            in_file_details['field-delimiter'] = os.pathsep
        in_data_frame.to_csv(path_or_buf=in_file_details['name'],
                             sep=in_file_details['field-delimiter'],
                             header=True,
                             index=False,
                             encoding='utf-8')

    @staticmethod
    def fn_save_data_frame_to_pickle(in_data_frame, in_file_details):
        if 'compression' not in in_file_details:
            in_file_details['compression'] = 'gzip'
        in_data_frame.to_pickle(path=in_file_details['name'],
                                compression=in_file_details['compression'])

    def fn_store_data_frame_to_file(self, local_logger, timmer, in_data_frame, in_file_details):
        timmer.start()
        is_file_saved = False
        given_format = self.fn_store_data_frame_to_file_validation(local_logger, in_file_details)
        if given_format == 'csv':
            self.fn_save_data_frame_to_csv(in_data_frame, in_file_details)
            is_file_saved = True
        elif given_format == 'excel':
            in_data_frame.to_excel(excel_writer=in_file_details['name'],
                                   engine='xlsxwriter',
                                   freeze_panes=(1, 1),
                                   verbose=True)
            is_file_saved = True
        elif given_format == 'pickle':
            self.fn_save_data_frame_to_pickle(in_data_frame, in_file_details)
            is_file_saved = True
        if is_file_saved:
            local_logger.info(self.lcl.gettext(
                'Pandas Data Frame has just been saved to file "{file_name}", '
                + 'considering {file_type} as file type')
                              .replace('{file_name}', in_file_details['name'])
                              .replace('{file_type}', in_file_details['format']))
        timmer.stop()

    def fn_store_data_frame_to_file_validation(self, local_logger, in_file_details):
        are_settings_ok = False
        if 'format' in in_file_details:
            implemented_file_formats = ['csv', 'excel', 'pickle']
            given_format = in_file_details['format'].lower()
            if given_format in implemented_file_formats:
                are_settings_ok = given_format
            else:
                local_logger.error(self.lcl.gettext(
                        'File "format" attribute has a value of "{format_value}" '
                        + 'which is not among currently implemented values: '
                        + '"{implemented_file_formats}", therefore file saving is not possible')
                                   .replace('{format_value}', given_format)
                                   .replace('{implemented_file_formats}',
                                            '", "'.join(implemented_file_formats)))
        else:
            local_logger.error(self.lcl.gettext('File "format" attribute is mandatory '
                                                + 'in the file setting, but missing, '
                                                + 'therefore file saving is not possible'))
        return are_settings_ok
