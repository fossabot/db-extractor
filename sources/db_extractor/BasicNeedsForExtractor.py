"""
Basic Needs For Extractor class

Handling specific needs for Extractor script
"""
# package to facilitate time operations
from datetime import datetime
# package to add support for multi-language (i18n)
import gettext
# package to handle files/folders and related metadata/operations
import os
# package to facilitate working with directories and files
from pathlib import Path
# package to facilitate common operations
from common.BasicNeeds import BasicNeeds


class BasicNeedsForExtractor:
    connection_details = None
    local_c_bn = None
    lcl = None
    str_ss = None
    srv = None

    def __init__(self, default_language='en_US'):
        self.local_c_bn = BasicNeeds(default_language)
        current_script = os.path.basename(__file__).replace('.py', '')
        lang_folder = os.path.join(os.path.dirname(__file__), current_script + '_Locale')
        self.lcl = gettext.translation(current_script, lang_folder, languages=[default_language])

    def fn_check_inputs_specific(self, input_parameters):
        self.local_c_bn.fn_validate_single_value(input_parameters.input_source_system_file,
                                             'file', self.lcl.gettext('source system file'))
        self.local_c_bn.fn_validate_single_value(input_parameters.input_credentials_file,
                                             'file', self.lcl.gettext('credentials file'))
        self.local_c_bn.fn_validate_single_value(input_parameters.input_extracting_sequence_file,
                                             'file', self.lcl.gettext('extracting sequence file'))

    def fn_validate_mandatory_properties(self, local_logger, who, properties_parent,
                                         list_properties):
        is_valid = True
        for current_property in list_properties:
            if current_property not in properties_parent:
                local_logger.error(self.lcl.gettext( \
                    '{who} does not contain "{current_property}" which is mandatory, '
                    + 'therefore extraction sequence will be ignored').replace('{who}', who) \
                                   .replace('{current_property}', current_property))
                is_valid = False
        return is_valid

    def fn_is_extraction_necessary(self, local_logger, relevant_details):
        extraction_is_necessary = False
        local_logger.debug(self.lcl.gettext('Extract behaviour is set to {extract_behaviour}') \
                           .replace('{extract_behaviour}', relevant_details['extract-behaviour']))
        if relevant_details['extract-behaviour'] == 'skip-if-output-file-exists':
            if Path(relevant_details['output-csv-file']).is_file():
                local_logger.debug(self.lcl.gettext('File {file_name} already exists, '
                                   + 'so database extraction will not be performed') \
                                   .replace('{file_name}', relevant_details['output-csv-file']))
            else:
                extraction_is_necessary = True
                local_logger.debug(self.lcl.gettext('File {file_name} does not exist, '
                                   + 'so database extraction has to be performed') \
                                   .replace('{file_name}', relevant_details['output-csv-file']))
        elif relevant_details['extract-behaviour'] == 'overwrite-if-output-file-exists':
            extraction_is_necessary = True
            local_logger.debug(self.lcl.gettext('Query extraction probably should be performed '
                                                + '(other checks might be required)'))
        return extraction_is_necessary

    def fn_is_extraction_neccesary_additional(self, local_logger, c_ph, c_fo, in_dict):
        if in_dict['session']['extract-overwrite-condition'] == 'inherit-from-parent':
            in_dict['session']['extract-overwrite-condition'] = \
                in_dict['query']['extract-overwrite-condition']
        elif in_dict['session']['extract-overwrite-condition'] == 'inherit-from-grand-parent':
            in_dict['session']['extract-overwrite-condition'] = \
                in_dict['sequence']['extract-overwrite-condition']
        ref_expr = in_dict['session']['extract-overwrite-condition']['reference-expression']
        reference_datetime = c_ph.eval_expression(
            local_logger, ref_expr, in_dict['session']['start-isoweekday'])
        child_parent_expressions = c_ph.get_child_parent_expressions()
        deviation_original = child_parent_expressions.get(ref_expr.split('_')[1])
        r_dt = datetime.strptime(reference_datetime,
                                 c_ph.output_standard_formats.get(deviation_original))
        return c_fo.fn_get_file_datetime_verdict(
            local_logger, in_dict['file']['name'], 'last modified', r_dt)

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

    def validate_all_json_files(self, local_logger, timmer, extracting_sequences):
        timmer.start()
        are_json_files_valid = False
        # validation of the extraction sequence file
        if self.validate_extraction_sequence_file(local_logger, extracting_sequences):
            are_json_files_valid = True
            local_logger.debug(self.lcl.gettext('Relevant JSON files are valid'))
        timmer.stop()
        return are_json_files_valid

    def validate_all_json_files_current(self, local_logger, timmer, extracting_sequence, seq_index,
                                        in_source_systems, in_user_secrets):
        # setting initial checks statuses
        can_proceed = {
            'Extraction'              : False,
            'Source System'           : False,
            'Source System Properties': False,
            'User Secrets'            : False,
            'User Secrets Properties' : False,
        }
        timmer.start()
        srv = {}
        # actual check
        can_proceed['Extraction'] = self.validate_extraction_sequence(local_logger,
                                                                      extracting_sequence)
        if can_proceed['Extraction']:
            # just few values that's going to be used a lot
            srv = {
                'vdtp': extracting_sequence['server-vendor']
                        + ' ' + extracting_sequence['server-type'],
                'vdr' : extracting_sequence['server-vendor'],
                'typ' : extracting_sequence['server-type'],
                'grp' : extracting_sequence['server-group'],
                'lyr' : extracting_sequence['server-layer'],
            }
            ac = extracting_sequence['account-label']
            can_proceed['Source System'] = \
                self.validate_current_source_system(local_logger, srv, in_source_systems)
            if can_proceed['Source System']:
                # variable for source server details
                ss = in_source_systems[srv['vdr']][srv['typ']]['Server'][srv['grp']][srv['lyr']]
                self.str_ss = '"' + '", "'.join(srv.values()) + '"'
                can_proceed['Source System Properties'] = \
                    self.validate_current_source_system_properties(local_logger, ss)
                can_proceed['User Secrets'] = \
                    self.validate_user_secrets_file(local_logger, srv, in_user_secrets)
                if can_proceed['Source System Properties'] and can_proceed['User Secrets']:
                    # variable with credentials for source server
                    u_dtl = in_user_secrets[srv['vdr']][srv['typ']][srv['grp']][srv['lyr']][ac]
                    can_proceed['User Secrets Properties'] = \
                        self.validate_user_secrets(local_logger, u_dtl)
                    self.connection_details = {
                        'server-vendor-and-type': srv['vdtp'],
                        'server-layer'          : srv['lyr'],
                        'ServerName'            : ss['ServerName'],
                        'ServerPort'            : int(ss['ServerPort']),
                        'Username'              : u_dtl['Username'],
                        'Name'                  : u_dtl['Name'],
                        'Password'              : u_dtl['Password'],
                    }
        local_logger.debug(self.lcl.gettext('For the sequence number {sequence_number} '
                                            + 'the relevant details for database connection '
                                            + 'have been verified')
                           .replace('{sequence_number}', str(seq_index)))
        timmer.stop()
        return can_proceed

    def validate_extraction_query(self, local_logger, timmer, in_extraction_sequence):
        timmer.start()
        mandatory_props_q = [
            'input-query-file',
            'sessions',
        ]
        is_valid = self.fn_validate_mandatory_properties(local_logger,
                                                         self.lcl.gettext('Extraction Query'),
                                                         in_extraction_sequence,
                                                         mandatory_props_q)
        timmer.stop()
        return is_valid

    def validate_extraction_sequence_file(self, local_logger, in_extract_sequence):
        is_valid = True
        if type(in_extract_sequence) != list:
            local_logger.error(self.lcl.gettext( \
                'Extraction sequence file name is not a LIST and must be, '
                + 'therefore cannot be used'))
            is_valid = False
        elif len(in_extract_sequence) < 1:
            local_logger.error(self.lcl.gettext('Extraction sequence file name is a LIST '
                               + 'but does not contain at least 1 extraction sequence, '
                               + 'therefore cannot be used'))
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
        is_valid = self.fn_validate_mandatory_properties(local_logger,
                                                         self.lcl.gettext('Extraction Sequence'),
                                                         in_extraction_sequence,
                                                         mandatory_props_e)
        return is_valid

    def validate_query_session(self, local_logger, in_session):
        mandatory_properties = [
            'extract-behaviour',
            'output-file',
        ]
        is_valid = self.fn_validate_mandatory_properties(local_logger,
                                                         self.lcl.gettext('Query Session')
                                                         + ' ' + self.str_ss,
                                                         in_session, mandatory_properties)
        return is_valid

    def validate_current_source_system_properties(self, local_logger, in_source_system):
        mandatory_properties = [
            'ServerName',
            'ServerPort',
        ]
        is_valid = self.fn_validate_mandatory_properties(local_logger,
                                                         self.lcl.gettext('Source System')
                                                         + ' ' + self.str_ss,
                                                         in_source_system, mandatory_properties)
        return is_valid

    def validate_current_source_system(self, local_logger, in_seq, in_source_systems):
        can_proceed_s = self.fn_validate_mandatory_properties(local_logger,
                                                              self.lcl.gettext('Source Systems'),
                                                              in_source_systems, [in_seq['vdr']])
        can_proceed_s1 = False
        if can_proceed_s:
            item_checked = in_source_systems[in_seq['vdr']]
            can_proceed_s1 = self.fn_validate_mandatory_properties(local_logger,
                                                                   self.lcl.gettext( \
                                                                       'Source Systems') + ' "'
                                                                   + in_seq['vdr'] + '"',
                                                                   item_checked, [in_seq['typ']])
        can_proceed_s2 = False
        if can_proceed_s1:
            item_checked = in_source_systems[in_seq['vdr']][in_seq['typ']]['Server']
            can_proceed_s2 = self.fn_validate_mandatory_properties(local_logger,
                                                                   self.lcl.gettext( \
                                                                       'Source Systems') + ' "'
                                                                   + in_seq['vdr']
                                                                   + '", "' + in_seq['typ']
                                                                   + '", "Server" ',
                                                                   item_checked, [in_seq['grp']])
        can_proceed_s3 = False
        if can_proceed_s2:
            item_checked = in_source_systems[in_seq['vdr']][in_seq['typ']]['Server'][in_seq['grp']]
            can_proceed_s3 = self.fn_validate_mandatory_properties(local_logger,
                                                                   self.lcl.gettext( \
                                                                       'Source Systems') + ' "'
                                                                   + in_seq['vdr']
                                                                   + '", "' + in_seq['typ']
                                                                   + '", "' + in_seq['grp']
                                                                   + '", "Server" ',
                                                                   item_checked, [in_seq['lyr']])
        return can_proceed_s and can_proceed_s1 and can_proceed_s2 and can_proceed_s3

    def validate_user_secrets(self, local_logger, in_user_secrets):
        mandatory_properties = [
            'Name',
            'Username',
            'Password',
        ]
        is_valid = self.fn_validate_mandatory_properties(local_logger,
                                                         self.lcl.gettext('User Secrets')
                                                         + ' ' + self.str_ss,
                                                         in_user_secrets, mandatory_properties)
        if in_user_secrets['Name'] in ('login', 'Your Full Name Here'):
            local_logger.warning(self.lcl.gettext('For {str_user_secrets} your "Name" property '
                                                  + 'has the default value: "{default_name_value}" '
                                                  + 'which is unusual') \
                                 .replace('{str_user_secrets}', self.str_ss) \
                                 .replace('{default_name_value}', in_user_secrets['Name']))
        if in_user_secrets['Username'] in ('usrnme', 'your_username_goes_here'):
            local_logger.warning(self.lcl.gettext('For {str_user_secrets} your "Username" property '
                                                  + 'has the default value: '
                                                  + '"{default_username_value}" '
                                                  + 'which is unusual') \
                                 .replace('{str_user_secrets}', self.str_ss) \
                                 .replace('{default_username_value}', in_user_secrets['Username']))
        if in_user_secrets['Password'] in ('pwd', 'your_password_goes_here'):
            local_logger.warning(self.lcl.gettext('For {str_user_secrets} your "Password" property '
                                                  + 'has the default value: '
                                                  + '"{default_password_value}" '
                                                  + 'which is unusual') \
                                 .replace('{str_user_secrets}', self.str_ss) \
                                 .replace('{default_password_value}', in_user_secrets['Password']))
        return is_valid

    def validate_user_secrets_file(self, local_logger, in_seq, in_user_secrets):
        can_proceed_u = self.fn_validate_mandatory_properties(local_logger,
                                                              self.lcl.gettext('User Secrets'),
                                                              in_user_secrets, [in_seq['vdr']])
        can_proceed_u1 = False
        if can_proceed_u:
            item_checked = in_user_secrets[in_seq['vdr']]
            can_proceed_u1 = self.fn_validate_mandatory_properties(local_logger,
                                                                   self.lcl.gettext('User Secrets')
                                                                   + ' "' + in_seq['vdr'] + '"',
                                                                   item_checked, [in_seq['typ']])
        can_proceed_u2 = False
        if can_proceed_u1:
            item_checked = in_user_secrets[in_seq['vdr']][in_seq['typ']]
            can_proceed_u2 = self.fn_validate_mandatory_properties(local_logger,
                                                                   self.lcl.gettext('User Secrets')
                                                                   + ' "' + in_seq['vdr']
                                                                   + '", "' + in_seq['typ']
                                                                   + '", "Server" ',
                                                                   item_checked, [in_seq['grp']])
        can_proceed_u3 = False
        if can_proceed_u2:
            item_checked = in_user_secrets[in_seq['vdr']][in_seq['typ']][in_seq['grp']]
            can_proceed_u3 = self.fn_validate_mandatory_properties(local_logger,
                                                                   self.lcl.gettext('User Secrets')
                                                                   + ' "' + in_seq['vdr']
                                                                   + '", "' + in_seq['typ']
                                                                   + '", "' + in_seq['grp']
                                                                   + '", "Server" ',
                                                                   item_checked, [in_seq['lyr']])
        return can_proceed_u and can_proceed_u1 and can_proceed_u2 and can_proceed_u3
