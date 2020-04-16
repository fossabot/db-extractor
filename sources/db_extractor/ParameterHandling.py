"""
Class Parameter Handling

Facilitates handling parameters values
"""
# package to facilitate common operations
from db_extractor.BasicNeeds import date, datetime, re, timedelta, BasicNeeds
# package to allow year and/or month operations based on a reference date
import datedelta


class ParameterHandling:
    lcl_bn = None
    known_expressions = {
        'year' : ['CY', 'CurrentYear'],
        'month': ['CYCM', 'CurrentYearCurrentMonth', 'CM'],
        'week' : ['CYCW', 'CurrentYearCurrentWeek', 'CW'],
        'day' : ['CYCMCD', 'CurrentYearCurrentMonthCurrentDay', 'CD']
    }

    def __init__(self):
        self.lcl_bn = BasicNeeds()

    def build_parameters(self, local_logger, query_session_parameters, in_parameter_rules):
        local_logger.debug('Seen Parameters are: ' + str(query_session_parameters))
        parameters_type = type(query_session_parameters)
        local_logger.debug('Parameters type is ' + str(parameters_type))
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
        if len(expression_parts) >= 3:
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
            final_string = self.interpret_known_expression(local_logger, expression_parts)
        else:
            local_logger.error('Unknown expression encountered '
                               + str(expression_parts[1]) + '...')
            exit(1)
        return final_string

    def get_flattened_known_expressions(self):
        flat_values = []
        index_counter = 0
        for current_expression_group in self.known_expressions.items():
            for current_expression in current_expression_group[1]:
                flat_values.append(index_counter)
                flat_values[index_counter] = current_expression
                index_counter += 1
        return flat_values

    def handle_query_parameters(self, local_logger, given_session):
        tp = None
        if 'parameters' in given_session:
            parameter_rules = []
            if 'parameters-handling-rules' in given_session:
                parameter_rules = given_session['parameters-handling-rules']
            tp = self.build_parameters(local_logger, given_session['parameters'], parameter_rules)
        return tp

    @staticmethod
    def handle_prefix_for_date_format(date_expression, expected_prefixes, in_date):
        prefix_for_date_format = ''
        matching_rule_short = re.search(r'CY', date_expression)
        matching_rule_long = re.search(r'CurrentYear', date_expression)
        if matching_rule_short or matching_rule_long:
            prefix_for_date_format = str(datetime.isocalendar(in_date)[0])
        if expected_prefixes == 'CYCM':
            matching_rule2_short = re.search(r'CM', date_expression)
            matching_rule2_long = re.search(r'CurrentMonth', date_expression)
            if matching_rule2_short or matching_rule2_long:
                prefix_for_date_format += datetime.strftime(in_date, '%m')
        return prefix_for_date_format

    def interpret_known_expression(self, local_logger, expression_parts):
        final_string = ''
        current_date = date.today()
        local_logger.debug('I have just been provided with a known expression "'
                           + '_'.join(expression_parts) + '" to interpret')
        if expression_parts[1] in self.known_expressions['year']:
            finalized_date = self.calculate_date_deviation(current_date, 'year', expression_parts)
            final_string = datetime.strftime(finalized_date, '%Y')
        elif expression_parts[1] in self.known_expressions['month']:
            finalized_date = self.calculate_date_deviation(current_date, 'month', expression_parts)
            final_string = self.handle_prefix_for_date_format(expression_parts[1], 'CY',
                                                              finalized_date) \
                           + datetime.strftime(finalized_date, '%m')
        elif expression_parts[1] in self.known_expressions['week']:
            finalized_date = self.calculate_date_deviation(current_date, 'week', expression_parts)
            week_iso_num = datetime.isocalendar(finalized_date)[1]
            final_string = self.handle_prefix_for_date_format(expression_parts[1], 'CY',
                                                              finalized_date) \
                           + self.lcl_bn.fn_numbers_with_leading_zero(str(week_iso_num), 2)
        elif expression_parts[1] in self.known_expressions['day']:
            finalized_date = self.calculate_date_deviation(current_date, 'day', expression_parts)
            final_string = self.handle_prefix_for_date_format(expression_parts[1], 'CYCM',
                                                              finalized_date) \
                           + datetime.strftime(finalized_date, '%d')
        local_logger.debug('Provided known expression "' + '_'.join(expression_parts)
                           + '" has been determined to be "' + final_string + '"')
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
                local_logger.info('Query with parameters interpreted is: '
                                  + self.lcl_bn.fn_multi_line_string_to_single_line(return_query))
            except TypeError as e:
                local_logger.debug('Initial query expects ' + str(in_parameters_number)
                                   + ' parameters but only ' + str(len(in_tp))
                                   + ' parameters were provided!')
                local_logger.error(e)
        timered.stop()

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
