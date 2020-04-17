"""
Class Parameter Handling

Facilitates handling parameters values
"""
# package to handle date and times
from datetime import date, datetime, timedelta
# package regular expressions
import re
# package to allow year and/or month operations based on a reference date
import datedelta


class ParameterHandling:
    known_expressions = {
        'year': ['CY', 'CurrentYear'],
        'month': ['CYCM', 'CurrentYearCurrentMonth'],
        'just_month': ['CM', 'CurrentMonth'],
        'week': ['CYCW', 'CurrentYearCurrentWeek'],
        'just_week': ['CW', 'CurrentWeek'],
        'day': ['CYCMCD', 'CurrentYearCurrentMonthCurrentDay'],
        'just_day': ['CD', 'CurrentDay']
    }

    def build_parameters(self, local_logger, query_session_parameters, in_parameter_rules):
        local_logger.debug('Seen Parameters are: ' + str(query_session_parameters))
        parameters_type = type(query_session_parameters)
        local_logger.debug('Parameters type is ' + str(parameters_type))
        tp = None
        if str(parameters_type) == "<class 'dict'>":
            tp = tuple(query_session_parameters.values())
        elif str(parameters_type) == "<class 'list'>":
            tp = tuple(query_session_parameters)
        else:
            local_logger.error('Unknown parameter type (expected either Dictionary or List)')
            exit(1)
        local_logger.debug('Initial Tuple for Parameters is: ' + str(tp))
        return self.stringify_parameters(local_logger, tp, in_parameter_rules)

    @staticmethod
    def calculate_date_deviation(in_date, deviation_type, expression_parts):
        final_date = in_date
        if deviation_type == 'year':
            final_date = in_date + datedelta.datedelta(years=int(expression_parts[2]))
        elif deviation_type == 'month':
            final_date = in_date + datedelta.datedelta(months=int(expression_parts[2]))
        elif deviation_type == 'week':
            final_date = in_date + timedelta(weeks=int(expression_parts[2]))
        elif deviation_type == 'day':
            final_date = in_date + timedelta(days=int(expression_parts[2]))
        return final_date

    def calculate_date_from_expression(self, local_logger, expression_parts):
        final_string = ''
        all_known_expressions = self.get_flattened_known_expressions()
        if expression_parts[1] in all_known_expressions:
            local_logger.debug('I have just been provided with a known expression "'
                               + '_'.join(expression_parts) + '" to interpret')
            final_string = self.interpret_known_expression(date.today(), expression_parts)
            local_logger.debug('Provided known expression "' + '_'.join(expression_parts)
                               + '" has been determined to be "' + final_string + '"')
        else:
            local_logger.error('Unknown expression encountered '
                               + str(expression_parts[1]) + '...')
            exit(1)
        return final_string

    def get_child_parent_expressions(self):
        child_parent_values = {}
        for current_expression_group in self.known_expressions.items():
            for current_expression in current_expression_group[1]:
                child_parent_values[current_expression] = current_expression_group[0]
        return child_parent_values

    def get_flattened_known_expressions(self):
        flat_values = []
        index_counter = 0
        for current_expression_group in self.known_expressions.items():
            for current_expression in current_expression_group[1]:
                flat_values.append(index_counter)
                flat_values[index_counter] = current_expression
                index_counter += 1
        return flat_values

    @staticmethod
    def get_week_number_as_two_digits_string(in_date):
        week_iso_num = datetime.isocalendar(in_date)[1]
        value_to_return = str(week_iso_num)
        if week_iso_num < 10:
            value_to_return = '0' + value_to_return
        return value_to_return

    def handle_query_parameters(self, local_logger, given_session):
        tp = None
        if 'parameters' in given_session:
            parameter_rules = []
            if 'parameters-handling-rules' in given_session:
                parameter_rules = given_session['parameters-handling-rules']
            tp = self.build_parameters(local_logger, given_session['parameters'], parameter_rules)
        return tp

    def interpret_known_expression(self, ref_date, expression_parts):
        child_parent_expressions = self.get_child_parent_expressions()
        deviation_original = child_parent_expressions.get(expression_parts[1])
        deviation = deviation_original.replace('just_', '')
        finalized_date = ref_date
        if len(expression_parts) >= 3:
            finalized_date = self.calculate_date_deviation(ref_date, deviation, expression_parts)
        week_number_string = self.get_week_number_as_two_digits_string(finalized_date)
        if deviation_original == 'week':
            final_string = str(datetime.isocalendar(finalized_date)[0]) + week_number_string
        elif deviation_original == 'just_week':
            final_string = week_number_string
        else:
            standard_formats = {
                'year': '%Y',
                'month': '%Y%m',
                'just_month': '%m',
                'day': '%Y%m%d',
                'just_day': '%d',
            }
            target_format = standard_formats.get(deviation_original)
            final_string = datetime.strftime(finalized_date, target_format)
        return final_string

    @staticmethod
    def manage_parameter_value(given_prefix, given_parameter, given_parameter_rules):
        element_to_join = ''
        if given_prefix == 'dict':
            element_to_join = given_parameter.values()
        elif given_prefix == 'list':
            element_to_join = given_parameter
        return given_parameter_rules[given_prefix + '-values-prefix'] \
               + given_parameter_rules[given_prefix + '-values-glue'].join(element_to_join) \
               + given_parameter_rules[given_prefix + '-values-suffix']

    def simulate_final_query(self, local_logger, timered, in_query, in_parameters_number, in_tp):
        timered.start()
        return_query = in_query
        if in_parameters_number > 0:
            try:
                return_query = in_query % in_tp
            except TypeError as e:
                local_logger.debug('Initial query expects ' + str(in_parameters_number)
                                   + ' parameters but only ' + str(len(in_tp))
                                   + ' parameters were provided!')
                local_logger.error(e)
        timered.stop()
        return return_query

    def special_case_string(self, local_logger, crt_parameter):
        resulted_parameter_value = crt_parameter
        matching_rule = re.search(r'(CalculatedDate\_[A-Za-z]{2,62}\_*(-*)[0-9]{0,2})', crt_parameter)
        if matching_rule:
            parameter_value_parts = matching_rule.group().split('_')
            calculated_parameter_value = self.calculate_date_from_expression(local_logger,
                                                                             parameter_value_parts)
            resulted_parameter_value = re.sub(matching_rule.group(), calculated_parameter_value,
                                              crt_parameter)
            local_logger.debug('Current Parameter is STR '
                               + 'and has been re-interpreted as value: '
                               + str(resulted_parameter_value))
        return resulted_parameter_value

    def stringify_parameters(self, local_logger, tuple_parameters, given_parameter_rules):
        working_list = []
        for ndx, crt_parameter in enumerate(tuple_parameters):
            current_parameter_type = str(type(crt_parameter))
            working_list.append(ndx)
            if current_parameter_type == "<class 'str'>":
                local_logger.debug('Current Parameter is STR and has the value: ' + crt_parameter)
                working_list[ndx] = self.special_case_string(local_logger, crt_parameter)
            elif current_parameter_type in ("<class 'list'>", "<class 'dict'>"):
                prefix = current_parameter_type.replace("<class '", '').replace("'>", '')
                local_logger.debug('Current Parameter is ' + prefix.upper()
                                   + ' and has the value: ' + str(crt_parameter))
                working_list[ndx] = self.manage_parameter_value(prefix.lower(), crt_parameter,
                                                                given_parameter_rules)
        final_tuple = tuple(working_list)
        local_logger.debug('Final Tuple for Parameters is: ' + str(final_tuple))
        return final_tuple
