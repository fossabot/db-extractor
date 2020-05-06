"""
Class Parameter Handling

Facilitates handling parameters values
"""
# package to handle date and times
from datetime import datetime, timedelta
# package to allow year and/or month operations based on a reference date
import datedelta
# package to add support for multi-language (i18n)
import gettext
# package to handle files/folders and related metadata/operations
import os
# package regular expressions
import re


class ParameterHandling:
    known_expressions = {
        'year': ['CY', 'CurrentYear'],
        'month': ['CYCM', 'CurrentYearCurrentMonth'],
        'just_month': ['CM', 'CurrentMonth'],
        'week': ['CYCW', 'CurrentYearCurrentWeek'],
        'just_week': ['CW', 'CurrentWeek'],
        'day': ['CYCMCD', 'CurrentYearCurrentMonthCurrentDay'],
        'just_day': ['CD', 'CurrentDay'],
        'hour': ['CYCMCDCH', 'CurrentYearCurrentMonthCurrentDayCurrentHour'],
    }
    output_standard_formats = {
        'year': '%Y',
        'month': '%Y%m',
        'just_month': '%m',
        'day': '%Y%m%d',
        'just_day': '%d',
        'hour': '%Y%m%d%H%M%S%f',
    }
    locale = None

    def __init__(self, in_language='en_US'):
        current_script = os.path.basename(__file__).replace('.py', '')
        lang_folder = os.path.join(os.path.dirname(__file__), current_script + '_Locale')
        self.locale = gettext.translation(current_script, lang_folder, languages=[in_language])

    def build_parameters(self, local_logger, query_session_parameters, in_parameter_rules
                         , in_start_iso_weekday):
        local_logger.debug(self.locale.gettext('Seen Parameters are: {parameters}')
                           .replace('{parameters}', str(query_session_parameters)))
        parameters_type = type(query_session_parameters)
        local_logger.debug(self.locale.gettext('Parameters type is {parameter_type}')
                           .replace('{parameter_type}', str(parameters_type)))
        tp = None
        if parameters_type == dict:
            tp = tuple(query_session_parameters.values())
        elif parameters_type == list:
            tp = tuple(query_session_parameters)
        else:
            local_logger.error(self.locale.gettext(
                'Unexpected parameter type, either Dictionary or List expected, '
                + 'but seen is {parameter_type}')
                               .replace('{parameter_type}', str(parameters_type)))
            exit(1)
        local_logger.debug(self.locale.gettext(
            'Initial Tuple for Parameters is: {parameters_tuple}')
                           .replace('{parameters_tuple}', str(tp)))
        return self.stringify_parameters(local_logger, tp, in_parameter_rules, in_start_iso_weekday)

    @staticmethod
    def calculate_date_deviation(in_date, deviation_type, expression_parts):
        if expression_parts[2] is None:
            expression_parts[2] = 0
        final_dates = {
            'year': in_date + datedelta.datedelta(years=int(expression_parts[2])),
            'month': in_date + datedelta.datedelta(months=int(expression_parts[2])),
            'week': in_date + timedelta(weeks=int(expression_parts[2])),
            'day': in_date + timedelta(days=int(expression_parts[2])),
            'hour': in_date + timedelta(hours=int(expression_parts[2])),
        }
        return final_dates.get(deviation_type)

    def calculate_date_from_expression(self, local_logger, expression_parts, in_start_iso_weekday):
        final_string = ''
        all_known_expressions = self.get_flattened_known_expressions()
        if expression_parts[1] in all_known_expressions:
            str_expression = '_'.join(expression_parts)
            local_logger.debug(self.locale.gettext(
                'A known expression "{expression_parts}" has to be interpreted')
                               .replace('{expression_parts}', str_expression))
            final_string = self.interpret_known_expression(
                    datetime.utcnow(), expression_parts, in_start_iso_weekday)
            local_logger.debug(self.locale.gettext(
                'Known expression "{expression_parts}" has been interpreted as {final_string}')
                               .replace('{expression_parts}', str_expression)
                               .replace('{final_string}', final_string))
        else:
            local_logger.debug(self.locale.gettext(
                'Unknown expression encountered: provided was "{given_expression}" '
                + 'which is not among known ones: "{all_known_expressions}"')
                               .replace('{given_expression}', str(expression_parts[1]))
                               .replace('{all_known_expressions}',
                                        '", "'.join(all_known_expressions)))
            exit(1)
        return final_string

    def eval_expression(self, local_logger, crt_parameter, in_start_iso_weekday):
        value_to_return = crt_parameter
        reg_ex = re.search(r'(CalculatedDate_[A-Za-z]{2,75}_*(-*)[0-9]{0,2})', crt_parameter)
        if reg_ex:
            parameter_value_parts = reg_ex.group().split('_')
            calculated = self.calculate_date_from_expression(
                    local_logger, parameter_value_parts, in_start_iso_weekday)
            value_to_return = re.sub(reg_ex.group(), calculated, crt_parameter)
            local_logger.debug(self.locale.gettext(
                'Current Parameter is STR and has been re-interpreted as value: "{str_value}"')
                               .replace('{str_value}', str(value_to_return)))
        return value_to_return

    def get_child_parent_expressions(self):
        child_parent_values = {}
        for current_expression_group in self.known_expressions.items():
            for current_expression in current_expression_group[1]:
                child_parent_values[current_expression] = current_expression_group[0]
        return child_parent_values

    def get_flattened_known_expressions(self):
        flat_values = []
        for crt_list in self.known_expressions.values():
            flat_values += crt_list
        return flat_values

    @staticmethod
    def get_week_number_as_two_digits_string(in_date, in_start_iso_weekday=1):
        if in_start_iso_weekday == 7:
            in_date = in_date + timedelta(days=1)
        week_iso_num = datetime.isocalendar(in_date)[1]
        iso_wek_to_return = str(week_iso_num)
        if week_iso_num < 10:
            iso_wek_to_return = '0' + iso_wek_to_return
        return str(datetime.isocalendar(in_date)[0]), iso_wek_to_return

    def handle_query_parameters(self, local_logger, given_session, in_start_iso_weekday):
        tp = None
        if 'parameters' in given_session:
            parameter_rules = []
            if 'parameters-handling-rules' in given_session:
                parameter_rules = given_session['parameters-handling-rules']
            tp = self.build_parameters(
                    local_logger, given_session['parameters'], parameter_rules,
                    in_start_iso_weekday)
        return tp

    def interpret_known_expression(self, ref_date, expression_parts, in_start_iso_weekday):
        child_parent_expressions = self.get_child_parent_expressions()
        deviation_original = child_parent_expressions.get(expression_parts[1])
        deviation = deviation_original.replace('just_', '')
        finalized_date = ref_date
        if len(expression_parts) > 2:
            finalized_date = self.calculate_date_deviation(ref_date, deviation, expression_parts)
        year_number_string, week_number_string = self.get_week_number_as_two_digits_string(
                finalized_date, in_start_iso_weekday)
        if deviation_original == 'week':
            final_string = year_number_string + week_number_string
        elif deviation_original == 'just_week':
            final_string = week_number_string
        else:
            final_string = datetime.strftime(
                    finalized_date, self.output_standard_formats.get(deviation_original))
        return final_string

    def manage_parameter_value(self, local_logger, given_prefix, given_parameter,
                               given_parameter_rules):
        element_to_join = ''
        if given_prefix == 'dict':
            element_to_join = given_parameter.values()
        elif given_prefix == 'list':
            element_to_join = given_parameter
        local_logger.debug(self.locale.gettext(
            'Current Parameter is {parameter_type} and has the value: "{str_value}"')
                           .replace('{parameter_type}', self.locale.gettext(given_prefix.upper()))
                           .replace('{str_value}', str(element_to_join)))
        return given_parameter_rules[given_prefix + '-values-prefix'] \
            + given_parameter_rules[given_prefix + '-values-glue'].join(element_to_join) \
            + given_parameter_rules[given_prefix + '-values-suffix']

    def simulate_final_query(self, local_logger, timer, in_query, in_parameters_number, in_tp):
        timer.start()
        return_query = in_query
        if in_parameters_number > 0:
            try:
                return_query = in_query % in_tp
            except TypeError as e:
                local_logger.error(self.locale.gettext(
                    'Initial query expects {expected_parameters_counted} '
                    + 'but only {given_parameters_counted} were provided')
                                   .replace('{expected_parameters_counted}',
                                            str(in_parameters_number))
                                   .replace('{given_parameters_counted}', str(len(in_tp))))
                local_logger.error(e)
        timer.stop()
        return return_query

    def stringify_parameters(self, local_logger, tuple_parameters, given_parameter_rules,
                             in_start_iso_weekday):
        working_list = []
        for ndx, crt_parameter in enumerate(tuple_parameters):
            current_parameter_type = type(crt_parameter)
            working_list.append(ndx)
            if current_parameter_type == str:
                local_logger.debug(self.locale.gettext(
                    'Current Parameter is {parameter_type} and has the value: "{str_value}"')
                                   .replace('{parameter_type}', self.locale.gettext('STR'))
                                   .replace('{str_value}', crt_parameter))
                working_list[ndx] = self.eval_expression(
                        local_logger, crt_parameter, in_start_iso_weekday)
            elif current_parameter_type in (list, dict):
                prefix = str(current_parameter_type).replace("<class '", '').replace("'>", '')
                working_list[ndx] = self.manage_parameter_value(
                        local_logger, prefix.lower(), crt_parameter, given_parameter_rules)
        final_tuple = tuple(working_list)
        local_logger.debug(self.locale.gettext('Final Tuple for Parameters is: {final_tuple}')
                           .replace('{final_tuple}', str(final_tuple)))
        return final_tuple
