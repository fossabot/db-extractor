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
from db_extractor.BasicNeeds import BasicNeeds


class BasicNeedsForExtractor:
    connection_details = None
    class_bn = None
    locale = None
    str_ss = None
    srv = None

    def __init__(self, in_language='en_US'):
        self.class_bn = BasicNeeds(in_language)
        file_parts = os.path.normpath(os.path.abspath(__file__)).replace('\\', os.path.altsep)\
            .split(os.path.altsep)
        locale_domain = file_parts[(len(file_parts)-1)].replace('.py', '')
        locale_folder = os.path.normpath(os.path.join(
            os.path.join(os.path.altsep.join(file_parts[:-2]), 'project_locale'), locale_domain))
        self.locale = gettext.translation(locale_domain, localedir=locale_folder,
                                          languages=[in_language], fallback=True)

    def fn_check_inputs_specific(self, in_parameters):
        self.class_bn.fn_validate_single_value(in_parameters.input_source_system_file, 'file')
        self.class_bn.fn_validate_single_value(in_parameters.input_credentials_file, 'file')
        self.class_bn.fn_validate_single_value(in_parameters.input_extracting_sequence_file, 'file')

    def fn_validate_mandatory_properties(self, local_logger, who, properties_parent, in_list):
        is_valid = True
        for current_property in in_list:
            if current_property not in properties_parent:
                local_logger.error(self.locale.gettext(
                    '{who} does not contain "{current_property}" which is mandatory, '
                    + 'therefore extraction sequence will be ignored').replace('{who}', who)
                                   .replace('{current_property}', current_property))
                is_valid = False
        return is_valid

    def fn_is_extraction_necessary(self, local_logger, relevant_details):
        extraction_is_necessary = False
        local_logger.debug(self.locale.gettext('Extract behaviour is set to {extract_behaviour}')
                           .replace('{extract_behaviour}', relevant_details['extract-behaviour']))
        if relevant_details['extract-behaviour'] == 'skip-if-output-file-exists':
            if Path(relevant_details['output-csv-file']).is_file():
                local_logger.debug(self.locale.gettext(
                        'File {file_name} already exists, '
                        + 'so database extraction will not be performed')
                                   .replace('{file_name}', relevant_details['output-csv-file']))
            else:
                extraction_is_necessary = True
                local_logger.debug(self.locale.gettext(
                        'File {file_name} does not exist, '
                        + 'so database extraction has to be performed')
                                   .replace('{file_name}', relevant_details['output-csv-file']))
        elif relevant_details['extract-behaviour'] == 'overwrite-if-output-file-exists':
            extraction_is_necessary = True
            local_logger.debug(self.locale.gettext('Query extraction probably should be performed '
                                                   + '(other checks might be required)'))
        return extraction_is_necessary

    @staticmethod
    def fn_is_extraction_necessary_additional(local_logger, c_ph, c_fo, in_dict):
        if in_dict['session']['extract-overwrite-condition'] == 'inherit-from-parent':
            in_dict['session']['extract-overwrite-condition'] = \
                in_dict['query']['extract-overwrite-condition']
        elif in_dict['session']['extract-overwrite-condition'] == 'inherit-from-grand-parent':
            in_dict['session']['extract-overwrite-condition'] = \
                in_dict['sequence']['extract-overwrite-condition']
        ref_expr = in_dict['session']['extract-overwrite-condition']['reference-expression']
        reference_datetime = c_ph.eval_expression(
            local_logger, ref_expr, in_dict['session']['start-iso-weekday'])
        child_parent_expressions = c_ph.get_child_parent_expressions()
        deviation_original = child_parent_expressions.get(ref_expr.split('_')[1])
        r_dt = datetime.strptime(
            reference_datetime, c_ph.output_standard_formats.get(deviation_original))
        return c_fo.fn_get_file_datetime_verdict(
            local_logger, in_dict['file']['name'], 'last modified', datetime.timestamp(r_dt))

    def fn_manage_individual_user_settings(self, local_logger, u, current_key):
        if current_key in u['Storage']:
            if u['Storage'][current_key] == 'environment variable':  # handling special value
                local_logger.debug(self.locale.gettext(
                    'As {current_key} has been set to be an "environment variable"'
                    + ', value will be read using {environment_variable_name}')
                                   .replace('{current_key}', current_key)
                                   .replace('{environment_variable_name}', u[current_key]))
                env_var_value = os.getenv(u[current_key])
                if env_var_value is None:
                    local_logger.error('No such {environment_variable_name} has been found')
                else:
                    u[current_key] = env_var_value
        return u

    def fn_manage_user_settings(self, local_logger, u):
        if 'Storage' in u:
            known_account_keys = ['Name', 'Password', 'Username']
            for current_key in known_account_keys:
                u = self.fn_manage_individual_user_settings(local_logger, u, current_key)
        return u

    def validate_all_json_files(self, local_logger, timer, extracting_sequences):
        timer.start()
        are_json_files_valid = False
        # validation of the extraction sequence file
        if self.validate_extraction_sequence_file(local_logger, extracting_sequences):
            are_json_files_valid = True
            local_logger.debug(self.locale.gettext('Relevant JSON files are valid'))
        timer.stop()
        return are_json_files_valid

    def validate_all_json_files_current(self, local_logger, timer, in_sequence, seq_index,
                                        in_source_systems, in_user_secrets):
        timer.start()
        # setting initial checks statuses
        can_proceed = {'Extraction'                    : self.validate_extraction_sequence(
            local_logger, in_sequence), 'Source System': False,
            'Source System Properties'                 : False, 'User Secrets': False,
            'User Secrets Properties'                  : False}
        # actual check
        if can_proceed['Extraction']:
            # just few values that's going to be used a lot
            srv = {
                'vdtp': in_sequence['server-vendor'] + ' ' + in_sequence['server-type'],
                'vdr' : in_sequence['server-vendor'],
                'typ' : in_sequence['server-type'],
                'grp' : in_sequence['server-group'],
                'lyr' : in_sequence['server-layer'],
            }
            can_proceed['Source System'] = self.validate_current_source_system(
                local_logger, srv, in_source_systems)
            if can_proceed['Source System']:
                # variable for source server details
                ss = in_source_systems[srv['vdr']][srv['typ']]['Server'][srv['grp']][srv['lyr']]
                self.str_ss = '"' + '", "'.join(srv.values()) + '"'
                can_proceed['Source System Properties'] = \
                    self.validate_current_source_system_properties(local_logger, ss)
                can_proceed['User Secrets'] = self.validate_user_secrets_file(
                    local_logger, srv, in_user_secrets)
                srv.update(can_proceed)
                srv.update(ss)
                srv.update({'ac': in_sequence['account-label']})
                can_proceed['User Secrets Properties'] = \
                    self.validate_user_and_establish_connection_details(
                        local_logger, srv, in_user_secrets)
        local_logger.debug(self.locale.gettext('For the sequence number {sequence_number} '
                                               + 'the relevant details for database connection '
                                               + 'have been verified')
                           .replace('{sequence_number}', str(seq_index)))
        timer.stop()
        return can_proceed

    def validate_extraction_query(self, local_logger, timer, in_extraction_sequence):
        timer.start()
        mandatory_props_q = [
            'input-query-file',
            'sessions',
        ]
        is_valid = self.fn_validate_mandatory_properties(local_logger,
                                                         self.locale.gettext('Extraction Query'),
                                                         in_extraction_sequence,
                                                         mandatory_props_q)
        timer.stop()
        return is_valid

    def validate_extraction_sequence_file(self, local_logger, in_extract_sequence):
        is_valid = True
        if type(in_extract_sequence) != list:
            local_logger.error(self.locale.gettext(
                'JSON extraction sequence is not a LIST and must be, therefore cannot be used'))
            is_valid = False
        elif len(in_extract_sequence) < 1:
            local_logger.error(self.locale.gettext(
                'Extraction sequence file name is a LIST '
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
        is_valid = self.fn_validate_mandatory_properties(
                local_logger, self.locale.gettext('Extraction Sequence'),
                in_extraction_sequence, mandatory_props_e)
        return is_valid

    def validate_query_session(self, local_logger, in_session):
        mandatory_properties = [
            'extract-behaviour',
            'output-file',
        ]
        is_valid = self.fn_validate_mandatory_properties(
                local_logger, self.locale.gettext('Query Session') + ' ' + self.str_ss,
                in_session, mandatory_properties)
        return is_valid

    def validate_current_source_system_properties(self, local_logger, in_source_system):
        mandatory_properties = [
            'ServerName',
            'ServerPort',
        ]
        is_valid = self.fn_validate_mandatory_properties(
                local_logger, self.locale.gettext('Source System') + ' ' + self.str_ss,
                in_source_system, mandatory_properties)
        return is_valid

    def validate_current_source_system(self, in_logger, in_seq, in_source_systems):
        can_proceed_s = self.fn_validate_mandatory_properties(
                in_logger, self.locale.gettext('Source Systems'),
                in_source_systems, [in_seq['vdr']])
        can_proceed_s1 = False
        if can_proceed_s:
            item_checked = in_source_systems[in_seq['vdr']]
            can_proceed_s1 = self.fn_validate_mandatory_properties(
                    in_logger, self.locale.gettext('Source Systems') + ' "' + in_seq['vdr'] + '"',
                    item_checked, [in_seq['typ']])
        can_proceed_s2 = False
        if can_proceed_s1:
            item_checked = in_source_systems[in_seq['vdr']][in_seq['typ']]['Server']
            can_proceed_s2 = self.fn_validate_mandatory_properties(
                    in_logger, self.locale.gettext('Source Systems') + ' "' + in_seq['vdr']
                    + '", "' + in_seq['typ'] + '", "Server" ',
                    item_checked, [in_seq['grp']])
        can_proceed_s3 = False
        if can_proceed_s2:
            item_checked = in_source_systems[in_seq['vdr']][in_seq['typ']]['Server'][in_seq['grp']]
            can_proceed_s3 = self.fn_validate_mandatory_properties(
                    in_logger, self.locale.gettext('Source Systems') + ' "' + in_seq['vdr']
                    + '", "' + in_seq['typ'] + '", "' + in_seq['grp'] + '", "Server" ',
                    item_checked, [in_seq['lyr']])
        return can_proceed_s and can_proceed_s1 and can_proceed_s2 and can_proceed_s3

    def validate_user_and_establish_connection_details(self, local_logger, in_dc, in_user):
        is_valid = False
        if in_dc['Source System Properties'] and in_dc['User Secrets']:
            # variable with credentials for source server
            raw_user_settings = in_user[in_dc['vdr']][in_dc['typ']][in_dc['grp']][in_dc['lyr']][in_dc['ac']]
            u = self.fn_manage_user_settings(local_logger, raw_user_settings)
            is_valid = self.validate_user_secrets(local_logger, u)
            self.connection_details = {
                'server-vendor-and-type': in_dc['vdtp'],
                'server-layer'          : in_dc['lyr'],
                'ServerName'            : in_dc['ServerName'],
                'ServerPort'            : int(in_dc['ServerPort']),
                'Username'              : u['Username'],
                'Name'                  : u['Name'],
                'Password'              : u['Password'],
            }
        return is_valid

    def validate_user_secrets(self, local_logger, in_user_secrets):
        mandatory_properties_dict = {
            'Name': {
                'tuple': ['login', 'Your Full Name Here'],
                'variable': {
                    'name': 'default_name_value',
                    'value': in_user_secrets['Name']
                }
            },
            'Username': {
                'tuple': ['usrnme', 'your_username_goes_here'],
                'variable': {
                    'name': 'default_username_value',
                    'value': in_user_secrets['Username']
                }
            },
            'Password': {
                'tuple': ['pwd', 'your_password_goes_here'],
                'variable': {
                    'name': 'default_password_value',
                    'value': in_user_secrets['Password']
                }
            }
        }
        mandatory_properties = list(mandatory_properties_dict.keys())
        is_valid = self.fn_validate_mandatory_properties(local_logger,
                                                         self.locale.gettext('User Secrets')
                                                         + ' ' + self.str_ss,
                                                         in_user_secrets, mandatory_properties)
        for current_property in mandatory_properties_dict:
            if in_user_secrets[current_property] in tuple(mandatory_properties_dict[current_property]['tuple']):
                local_logger.warning(
                    self.locale.gettext('For {str_user_secrets} your "' + current_property + '" property '
                                        + 'has the default value: "{'
                                        + mandatory_properties_dict[current_property]['variable']['name']
                                        + '}" which is unusual')
                        .replace('{str_user_secrets}', self.str_ss)
                        .replace(mandatory_properties_dict[current_property]['variable']['name']
                                 , mandatory_properties_dict[current_property]['variable']['value']))
        return is_valid

    def validate_user_secrets_file(self, local_logger, in_seq, in_user_secrets):
        can_proceed_u = self.fn_validate_mandatory_properties(
                local_logger, self.locale.gettext('User Secrets'),
                in_user_secrets, [in_seq['vdr']])
        can_proceed_u1 = False
        if can_proceed_u:
            item_checked = in_user_secrets[in_seq['vdr']]
            can_proceed_u1 = self.fn_validate_mandatory_properties(
                    local_logger, self.locale.gettext('User Secrets') + ' "' + in_seq['vdr'] + '"',
                    item_checked, [in_seq['typ']])
        can_proceed_u2 = False
        if can_proceed_u1:
            item_checked = in_user_secrets[in_seq['vdr']][in_seq['typ']]
            can_proceed_u2 = self.fn_validate_mandatory_properties(
                    local_logger, self.locale.gettext('User Secrets') + ' "' + in_seq['vdr']
                    + '", "' + in_seq['typ'] + '", "Server" ',
                    item_checked, [in_seq['grp']])
        can_proceed_u3 = False
        if can_proceed_u2:
            item_checked = in_user_secrets[in_seq['vdr']][in_seq['typ']][in_seq['grp']]
            can_proceed_u3 = self.fn_validate_mandatory_properties(
                    local_logger, self.locale.gettext('User Secrets') + ' "' + in_seq['vdr']
                    + '", "' + in_seq['typ'] + '", "' + in_seq['grp'] + '", "Server" ',
                    item_checked, [in_seq['lyr']])
        return can_proceed_u and can_proceed_u1 and can_proceed_u2 and can_proceed_u3
