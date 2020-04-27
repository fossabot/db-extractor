"""
Basic Needs For Extractor class

Handling specific needs for Extractor script
"""
from datetime import datetime
# package to facilitate working with directories and files
from pathlib import Path
# package to facilitate common operations
from common.BasicNeeds import BasicNeeds


class BasicNeedsForExtractor:
    lcl_bn = None

    def __init__(self):
        self.lcl_bn = BasicNeeds()

    def fn_check_inputs_specific(self, input_parameters):
        self.lcl_bn.fn_validate_single_value(input_parameters.input_source_system_file,
                                             'file', 'source system file')
        self.lcl_bn.fn_validate_single_value(input_parameters.input_credentials_file,
                                             'file', 'credentials file')
        self.lcl_bn.fn_validate_single_value(input_parameters.input_extracting_sequence_file,
                                             'file', 'extracting sequence file')

    @staticmethod
    def fn_validate_mandatory_properties(local_logger, who, properties_parent, list_properties):
        is_valid = True
        for current_property in list_properties:
            if current_property not in properties_parent:
                local_logger.error(who + ' does not contain '
                                   + f'"{current_property}" which is mandatory, '
                                   + 'therefore extraction sequence will be ignored')
                is_valid = False
        return is_valid

    @staticmethod
    def fn_is_extraction_necessary(local_logger, relevant_details):
        extraction_is_necessary = False
        local_logger.debug('Extract behaviour is set to ' + relevant_details['extract-behaviour'])
        if relevant_details['extract-behaviour'] == 'skip-if-output-file-exists':
            if Path(relevant_details['output-csv-file']).is_file():
                local_logger.debug('File ' + relevant_details['output-csv-file']
                                   + ' already exists, '
                                   + 'so database extraction will not be performed')
            else:
                extraction_is_necessary = True
                local_logger.debug('File ' + relevant_details['output-csv-file']
                                   + ' does not exist, '
                                   + 'so database extraction has to be performed')
        elif relevant_details['extract-behaviour'] == 'overwrite-if-output-file-exists':
            extraction_is_necessary = True
            local_logger.debug('Database extraction has to be performed')
        return extraction_is_necessary

    def fn_is_extraction_neccesary_additional(self, local_logger, c_ph, c_fo, crt_session):
        ref_expr = crt_session['extract-overwrite-condition']['reference-expresion']
        reference_datetime = c_ph.eval_expression(local_logger, ref_expr,
                                                  crt_session['start-isoweekday'])
        child_parent_expressions = c_ph.get_child_parent_expressions()
        deviation_original = child_parent_expressions.get(ref_expr.split('_')[1])
        r_dt = datetime.strptime(reference_datetime,
                                 c_ph.output_standard_formats.get(deviation_original))
        return c_fo.fn_get_file_datetime_verdict(local_logger, crt_session['output-file']['name'],
                                                 'last modified', r_dt)

    @staticmethod
    def fn_set_extract_behaviour(in_session):
        default_value = 'skip-if-output-file-exists'
        allowed_values = [
            'skip-if-output-file-exists',
            'overwrite-if-output-file-exists',
        ]
        if ('extract-behaviour' not in in_session) \
                or (in_session['extract-behaviour'] not in allowed_values):
            value_to_return = default_value
        else:
            value_to_return = in_session['extract-behaviour']
        return value_to_return

    def validate_extraction_query(self, local_logger, in_extraction_sequence):
        mandatory_props_q = [
            'input-query-file',
            'sessions',
        ]
        is_valid = self.fn_validate_mandatory_properties(local_logger, 'Extraction Query',
                                                         in_extraction_sequence,
                                                         mandatory_props_q)
        return is_valid

    @staticmethod
    def validate_extraction_sequence_file(local_logger, in_extract_sequence):
        is_valid = True
        if str(type(in_extract_sequence)) != "<class 'list'>":
            local_logger.error('Extraction sequence file name is not a LIST, '
                               + 'therefore cannot be used')
            is_valid = False
        elif len(in_extract_sequence) < 1:
            local_logger.error('Extraction sequence file name is a LIST '
                               + 'but does not contain at least 1 extraction sequence, '
                               + 'therefore cannot be used')
            is_valid = False
        return is_valid

    def validate_extraction_sequence(self, local_logger, in_extraction_sequence):
        mandatory_props_e = [
            'server-vendor',
            'server-type',
            'server-group',
            'server-layer',
            'account-label',
            'queries',
        ]
        is_valid = self.fn_validate_mandatory_properties(local_logger, 'Extraction Sequence',
                                                         in_extraction_sequence,
                                                         mandatory_props_e)
        return is_valid

    def validate_query_session(self, local_logger, str_query_session, in_session):
        mandatory_properties = [
            'extract-behaviour',
            'output-file',
        ]
        is_valid = self.fn_validate_mandatory_properties(local_logger,
                                                         'Query Session ' + str_query_session,
                                                         in_session, mandatory_properties)
        return is_valid

    def validate_source_system(self, local_logger, str_source_system, in_source_system):
        mandatory_properties = [
            'ServerName',
            'ServerPort',
        ]
        is_valid = self.fn_validate_mandatory_properties(local_logger,
                                                         'Source System ' + str_source_system,
                                                         in_source_system, mandatory_properties)
        return is_valid

    def validate_source_systems_file(self, local_logger, in_seq, in_source_systems):
        can_proceed_s = self.fn_validate_mandatory_properties(local_logger, 'Source Systems',
                                                              in_source_systems, [in_seq['vdr']])
        can_proceed_s1 = False
        if can_proceed_s:
            item_checked = in_source_systems[in_seq['vdr']]
            can_proceed_s1 = self.fn_validate_mandatory_properties(local_logger, 'Source Systems "'
                                                                   + in_seq['vdr'] + '"',
                                                                   item_checked, [in_seq['typ']])
        can_proceed_s2 = False
        if can_proceed_s1:
            item_checked = in_source_systems[in_seq['vdr']][in_seq['typ']]['Server']
            can_proceed_s2 = self.fn_validate_mandatory_properties(local_logger, 'Source Systems "'
                                                                   + in_seq['vdr']
                                                                   + '", "' + in_seq['typ']
                                                                   + '", "Server" ',
                                                                   item_checked, [in_seq['grp']])
        can_proceed_s3 = False
        if can_proceed_s2:
            item_checked = in_source_systems[in_seq['vdr']][in_seq['typ']]['Server'][in_seq['grp']]
            can_proceed_s3 = self.fn_validate_mandatory_properties(local_logger, 'Source Systems "'
                                                                   + in_seq['vdr']
                                                                   + '", "' + in_seq['typ']
                                                                   + '", "' + in_seq['grp']
                                                                   + '", "Server" ',
                                                                   item_checked, [in_seq['lyr']])
        return can_proceed_s and can_proceed_s1 and can_proceed_s2 and can_proceed_s3

    def validate_user_secrets(self, local_logger, str_user_secrets, in_user_secrets):
        mandatory_properties = [
            'Name',
            'Username',
            'Password',
        ]
        is_valid = self.fn_validate_mandatory_properties(local_logger,
                                                         'User Secrets ' + str_user_secrets,
                                                         in_user_secrets, mandatory_properties)
        if in_user_secrets['Name'] in ('login', 'Your Full Name Here'):
            local_logger.warning(f'For {str_user_secrets} your "Name" has the default value: "'
                                 + in_user_secrets['Name'] + '" which is unusual')
        if in_user_secrets['Username'] in ('usrnme', 'your_username_goes_here'):
            local_logger.warning(f'For {str_user_secrets} your "Username" has the default value: "'
                                 + in_user_secrets['Username'] + '" which is unusual')
        if in_user_secrets['Password'] in ('pwd', 'your_password_goes_here'):
            local_logger.warning(f'For {str_user_secrets} your "Password" has the default value: "'
                                 + in_user_secrets['Password'] + '" which is unusual')
        return is_valid

    def validate_user_secrets_file(self, local_logger, in_seq, in_user_secrets):
        can_proceed_u = self.fn_validate_mandatory_properties(local_logger, 'User Secrets',
                                                              in_user_secrets, [in_seq['vdr']])
        can_proceed_u1 = False
        if can_proceed_u:
            item_checked = in_user_secrets[in_seq['vdr']]
            can_proceed_u1 = self.fn_validate_mandatory_properties(local_logger, 'User Secrets "'
                                                                   + in_seq['vdr'] + '"',
                                                                   item_checked, [in_seq['typ']])
        can_proceed_u2 = False
        if can_proceed_u1:
            item_checked = in_user_secrets[in_seq['vdr']][in_seq['typ']]
            can_proceed_u2 = self.fn_validate_mandatory_properties(local_logger, 'User Secrets "'
                                                                   + in_seq['vdr']
                                                                   + '", "' + in_seq['typ']
                                                                   + '", "Server" ',
                                                                   item_checked, [in_seq['grp']])
        can_proceed_u3 = False
        if can_proceed_u2:
            item_checked = in_user_secrets[in_seq['vdr']][in_seq['typ']][in_seq['grp']]
            can_proceed_u3 = self.fn_validate_mandatory_properties(local_logger, 'User Secrets "'
                                                                   + in_seq['vdr']
                                                                   + '", "' + in_seq['typ']
                                                                   + '", "' + in_seq['grp']
                                                                   + '", "Server" ',
                                                                   item_checked, [in_seq['lyr']])
        return can_proceed_u and can_proceed_u1 and can_proceed_u2 and can_proceed_u3
