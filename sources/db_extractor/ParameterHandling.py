"""
Class Parameter Handling

Facilitates handling parameters values
"""
# package to handle dates and times
from datetime import datetime as ClassDT
from datetime import timedelta
# package to facilitate common operations
from db_extractor.BasicNeeds import BasicNeeds
# package to facilitate regular expressions
import re


class ParameterHandling:
    lcl_bn = None

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

    def calculate_date_from_expression(self, local_logger, expression_parts):
        final_string = ''
        current_date = ClassDT.today()
        if expression_parts[1] == 'CYCW':
            if len(expression_parts) >= 3:
                current_date = current_date + timedelta(weeks=int(expression_parts[2]))
            week_iso_num = ClassDT.isocalendar(current_date)[1]
            final_string = str(ClassDT.isocalendar(current_date)[0])\
                           + self.lcl_bn.fn_numbers_with_leading_zero(str(week_iso_num), 2)
        else:
            local_logger.error('Unknown expression encountered '
                               + str(expression_parts[1]) + '...')
            exit(1)
        return final_string

    def handle_query_parameters(self, local_logger, given_session):
        tp = None
        if 'parameters' in given_session:
            parameter_rules = []
            if 'parameters-handling-rules' in given_session:
                parameter_rules = given_session['parameters-handling-rules']
            tp = self.build_parameters(local_logger, given_session['parameters'], parameter_rules)
        return tp

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
        matching_rule = re.search(r'(CalculatedDate\_[A-Z]{4}\_(-*)[0-9]{1,2})', crt_parameter)
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
