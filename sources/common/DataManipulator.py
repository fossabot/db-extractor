"""
Data Manipulation class
"""
# package to add support for multi-language (i18n)
import gettext
# package to handle files/folders and related metadata/operations
import os


class DataManipulator:
    lcl = None

    def __init__(self, default_language='en_US'):
        current_script = os.path.basename(__file__).replace('.py', '')
        lang_folder = os.path.join(os.path.dirname(__file__), current_script + '_Locale')
        self.lcl = gettext.translation(current_script, lang_folder, languages=[default_language])

    def fn_add_and_shift_column(self, local_logger, timmer, input_data_frame,
                                input_details: list):
        for current_dict in input_details:
            c_dict = current_dict
            timmer.start()
            input_data_frame[c_dict['New Column']] = input_data_frame[c_dict['Original Column']]
            offset_sign = (lambda x: 1 if x == 'down' else -1)
            col_offset = offset_sign(c_dict['Direction']) * c_dict['Deviation']
            input_data_frame[c_dict['New Column']] = input_data_frame[c_dict['New Column']]\
                .shift(col_offset)
            input_data_frame[c_dict['New Column']] = input_data_frame[c_dict['New Column']]\
                .apply(lambda x: str(x).replace('.0', ''))\
                .apply(lambda x: str(x).replace('nan', str(c_dict['Empty Values Replacement'])))
            local_logger.info(self.lcl.gettext(
                'A new column named "{new_column_name}" as copy from "{original_column}" '
                + 'then shifted by {shifting_rows} to relevant data frame '
                + '(filling any empty value as {empty_values_replacement})')
                              .replace('{new_column_name}', c_dict['New Column'])
                              .replace('{original_column}', c_dict['Original Column'])
                              .replace('{shifting_rows}', str(col_offset))
                              .replace('{empty_values_replacement}',
                                       str(c_dict['Empty Values Replacement'])))
            timmer.stop()
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
    def fn_get_column_index_from_data_frame(data_frame_columns, column_name_to_identify):
        column_index_to_return = 0
        for ndx, column_name in enumerate(data_frame_columns):
            if column_name == column_name_to_identify:
                column_index_to_return = ndx
        return column_index_to_return

    @staticmethod
    def fn_get_first_and_last_column_value_from_data_frame(in_data_frame, in_column_name):
        return {
            'first': in_data_frame.iloc[0][in_column_name],
            'last': in_data_frame.iloc[(len(in_data_frame) - 1)][in_column_name],
        }
