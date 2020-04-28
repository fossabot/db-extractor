"""
BasicNeeds - useful functions library

This library has functions useful to keep main logic short and simple
"""
# package to handle date and times
from datetime import datetime, timedelta
# package to add support for multi-language (i18n)
import gettext
# package to handle files/folders and related metadata/operations
import os
# package regular expressions
import re


class BasicNeeds:
    lcl = None

    def __init__(self, default_language='en_US'):
        current_script = os.path.basename(__file__).replace('.py', '')
        lang_folder = os.path.join(os.path.dirname(__file__), current_script + '_Locale')
        self.lcl = gettext.translation(current_script, lang_folder, languages=[default_language])

    def fn_check_inputs(self, input_parameters):
        if input_parameters.output_log_file is not None:
            # checking log folder first as there's all further messages will be stored
            self.fn_validate_single_value(os.path.dirname(input_parameters.output_log_file),
                                          'folder', self.lcl.gettext('log file'))

    def fn_final_message(self, local_logger, log_file_name, performance_in_seconds):
        total_time_string = str(timedelta(seconds=performance_in_seconds))
        if log_file_name == 'None':
            self.fn_timestamped_print(self.lcl.gettext( \
                'Application finished, whole script took {total_time_string}') \
                                      .replace('{total_time_string}', total_time_string))
        else:
            local_logger.info(self.lcl.gettext('Total execution time was {total_time_string}') \
                              .replace('{total_time_string}', total_time_string))
            self.fn_timestamped_print(self.lcl.gettext( \
                'Application finished, for complete logged details please check {log_file_name}')
                                      .replace('{log_file_name}', log_file_name))

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
        print(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f %Z") + '- ' + string_to_print)

    def fn_validate_one_value(self, value_to_validate, validation_type, name_meaning):
        is_invalid = False
        message = ''
        if validation_type == 'file':
            is_invalid = (not os.path.isfile(value_to_validate))
            message = self.lcl.gettext( \
                'Given file "{value_to_validate}" does not exist') \
                .replace('{value_to_validate}', value_to_validate)
        elif validation_type == 'folder':
            is_invalid = (not os.path.isdir(value_to_validate))
            message = self.lcl.gettext( \
                'Given folder "{value_to_validate}" does not exist') \
                .replace('{value_to_validate}', value_to_validate)
        elif validation_type == 'url':
            url_reg_expression = 'https?://(?:www)?(?:[\\w-]{2,255}(?:\\.\\w{2,66}){1,2})'
            is_invalid = (not re.match(url_reg_expression, value_to_validate))
            message = self.lcl.gettext( \
                'Given url "{value_to_validate}" is not valid') \
                .replace('{value_to_validate}', value_to_validate)
        return {
            'is_invalid': is_invalid,
            'message': message,
        }

    def fn_validate_single_value(self, value_to_validate, validation_type, name_meaning):
        validation_details = self.fn_validate_one_value(value_to_validate, validation_type,
                                                        name_meaning)
        if validation_details['is_invalid']:
            self.fn_timestamped_print(validation_details['message'])
            exit(1)
