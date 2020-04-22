"""
BasicNeeds - useful functions library

This library has functions useful to keep main logic short and simple
"""
# package to handle date and times
from datetime import datetime, timedelta
# package to handle files/folders and related metadata/operations
import os
# package regular expressions
import re


class BasicNeeds:

    def fn_check_inputs(self, input_parameters):
        if input_parameters.output_log_file is not None:
            # checking log folder first as there's all further messages will be stored
            self.fn_validate_single_value(os.path.dirname(input_parameters.output_log_file),
                                          'folder', 'log file')

    def fn_final_message(self, local_logger, log_file_name, performance_in_seconds):
        total_time_string = str(timedelta(seconds=performance_in_seconds))
        if log_file_name == 'None':
            self.fn_timestamped_print('Application finished, whole script took '
                                      + total_time_string)
        else:
            local_logger.info(f'Total execution time was ' + total_time_string)
            self.fn_timestamped_print('Application finished, '
                                      + 'for complete logged details please check '
                                      + log_file_name)

    @staticmethod
    def fn_multi_line_string_to_single_line(input_string):
        string_to_return = input_string.replace('\n', ' ').replace('\r', ' ')
        return re.sub(r'\s{2,100}', ' ', string_to_return).replace(' , ', ', ').strip()

    @staticmethod
    def fn_numbers_with_leading_zero(input_number_as_string, digits):
        final_number = input_number_as_string
        if len(input_number_as_string) < digits:
            final_number = '0' * (digits - len(input_number_as_string)) + input_number_as_string
        return final_number

    def fn_optional_print(self, boolean_variable, string_to_print):
        if boolean_variable:
            self.fn_timestamped_print(string_to_print)

    @staticmethod
    def fn_timestamped_print(string_to_print):
        print(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f %Z") + ' - ' + string_to_print)

    @staticmethod
    def fn_validate_one_value(value_to_validate, validation_type, name_meaning):
        is_fatal_error = False
        message = ''
        if validation_type == 'file':
            is_fatal_error = (not os.path.isfile(value_to_validate))
            message = 'Given ' + name_meaning + ' "' + value_to_validate \
                      + '" does not exist, please check your inputs!'
        elif validation_type == 'folder':
            is_fatal_error = (not os.path.isdir(value_to_validate))
            message = 'Given ' + name_meaning + ' "' + value_to_validate \
                      + '" does not exist, please check your inputs!'
        elif validation_type == 'url':
            url_reg_expression = 'https?://(?:www)?(?:[\\w-]{2,255}(?:\\.\\w{2,66}){1,2})'
            is_fatal_error = (not re.match(url_reg_expression, value_to_validate))
            message = 'Given ' + name_meaning + ' "' + value_to_validate \
                      + '" does not seem a valid one, please check your inputs!'
        return {
            'is_fatal_error': is_fatal_error,
            'message': message,
        }

    def fn_validate_single_value(self, value_to_validate, validation_type, name_meaning):
        validation_details = self.fn_validate_one_value(value_to_validate, validation_type,
                                                        name_meaning)
        if validation_details['is_fatal_error']:
            self.fn_timestamped_print(validation_details['message'])
            exit(1)
