"""
Facilitates moving files from a specified directory and matching pattern to a destination directory
"""
# useful methods to measure time performance by small pieces of code
from codetiming import Timer
# package to add support for multi-language (i18n)
import gettext
# package to facilitate operating system operations
import os
# package to facilitate working with directories and files
from pathlib import Path
# common Custom classes
from common.FileOperations import FileOperations
from common.BasicNeeds import BasicNeeds
from common.CommandLineArgumentsManagement import CommandLineArgumentsManagement
from common.DataManipulator import DataManipulator
from common.LoggingNeeds import LoggingNeeds
# Custom classes specific to this package
from db_extractor.BasicNeedsForExtractor import BasicNeedsForExtractor
from db_extractor.DatabaseTalker import DatabaseTalker
from db_extractor.ParameterHandling import ParameterHandling
# get current script name
CURRENT_SCRIPT_NAME = os.path.basename(__file__).replace('.py', '')
SCRIPT_LANGUAGE = 'ro_RO'

# main execution logic
if __name__ == '__main__':
    # instantiate File Operations class
    c_fo = FileOperations(SCRIPT_LANGUAGE)
    # load application configuration (inputs are defined into a json file)
    crt_folder = os.path.dirname(__file__)
    configuration_file = os.path.join(crt_folder, 'config/db-extractor.json').replace('\\', '/')
    configuration_details = c_fo.fn_open_file_and_get_content(configuration_file)
    # instantiate Command Line Arguments class
    c_clam = CommandLineArgumentsManagement(SCRIPT_LANGUAGE)
    # get command line parameter values
    input_parameters = \
        c_clam.parse_arguments(configuration_details['input_options'][CURRENT_SCRIPT_NAME])
    # instantiate Basic Needs class
    c_bn = BasicNeeds(SCRIPT_LANGUAGE)
    # checking inputs, if anything is invalid an exit(1) will take place
    c_bn.fn_check_inputs(input_parameters)
    # instantiate Extractor Specific Needs class
    c_bnfe = BasicNeedsForExtractor()
    # checking inputs, if anything is invalid an exit(1) will take place
    c_bnfe.fn_check_inputs_specific(input_parameters)
    # instantiate Logger class
    c_ln = LoggingNeeds()
    # initiate logger
    c_ln.initiate_logger(input_parameters.output_log_file, CURRENT_SCRIPT_NAME)
    # initiate localization specific for this script
    lang_folder = os.path.join(os.path.dirname(__file__), CURRENT_SCRIPT_NAME + '_Locale')
    script_lcl = gettext.translation(CURRENT_SCRIPT_NAME, lang_folder, languages=[SCRIPT_LANGUAGE])
    # define global timer to use
    t = Timer(CURRENT_SCRIPT_NAME,
              text=script_lcl.gettext('Time spent is {seconds}'),
              logger=c_ln.logger.debug)
    # reflect title and input parameters given values in the log
    c_clam.listing_parameter_values(c_ln.logger, t, 'Database Extractor',
                                    configuration_details['input_options'][CURRENT_SCRIPT_NAME],
                                    input_parameters)
    # loading extracting sequence details
    t.start()
    extracting_sequences = c_fo.fn_open_file_and_get_content(
        input_parameters.input_extracting_sequence_file, 'json')
    c_ln.logger.info(script_lcl.gettext( \
        'Configuration file name with extracting sequence(es) has been loaded'))
    t.stop()
    c_fo.fn_store_file_statistics(c_ln.logger, t, input_parameters.input_extracting_sequence_file,
                                  script_lcl.gettext('Configuration file name '
                                                     + 'with extracting sequence(es)'))
    # validation of the extraction sequence file
    file_is_valid = c_bnfe.validate_extraction_sequence_file(c_ln.logger, extracting_sequences)
    if not file_is_valid:
        exit(1)
    # get the source system details from provided file
    t.start()
    source_systems = c_fo.fn_open_file_and_get_content(input_parameters.input_source_system_file,
                                                       'json')['Systems']
    c_ln.logger.info(script_lcl.gettext('Source Systems file name has been loaded'))
    t.stop()
    c_fo.fn_store_file_statistics(c_ln.logger, t, input_parameters.input_source_system_file,
                                  script_lcl.gettext('Source Systems file name'))
    # get the source system details from provided file
    t.start()
    configured_secrets = c_fo.fn_open_file_and_get_content(input_parameters.input_credentials_file,
                                                           'json')['Credentials']
    c_ln.logger.info(script_lcl.gettext('Configuration file name with credentials has been loaded'))
    t.stop()
    c_fo.fn_store_file_statistics(c_ln.logger, t, input_parameters.input_credentials_file,
                                  script_lcl.gettext('Configuration file name with credentials'))
    # instantiate Parameter Handling class
    c_ph = ParameterHandling(SCRIPT_LANGUAGE)
    # instantiate Data Manipulator class, useful to manipulate data frames
    c_dm = DataManipulator(SCRIPT_LANGUAGE)
    # instantiate Database Talker class
    c_dbtkr = DatabaseTalker(SCRIPT_LANGUAGE)
    # cycling through the configurations
    int_extracting_sequence = 1
    for crt_sequence in extracting_sequences:
        t.start()
        can_proceed_e = c_bnfe.validate_extraction_sequence(c_ln.logger, crt_sequence)
        srv = {}
        can_proceed_s = False
        src_srvr = {}
        can_proceed_ss = False
        if can_proceed_e:
            # just few values that's going to be used a lot
            srv = {
                'vdr': crt_sequence['server-vendor'],
                'typ': crt_sequence['server-type'],
                'grp': crt_sequence['server-group'],
                'lyr': crt_sequence['server-layer']
            }
            can_proceed_s = c_bnfe.validate_source_systems_file(c_ln.logger, srv, source_systems)
            can_proceed_u = False
            if can_proceed_s:
                # variable for source server details
                src_srvr = source_systems[srv['vdr']][srv['typ']]['Server'][srv['grp']][srv['lyr']]
                str_ss = '"' + '", "'.join(srv.values()) + '"'
                can_proceed_ss = c_bnfe.validate_source_system(c_ln.logger, str_ss, src_srvr)
                can_proceed_u = c_bnfe.validate_user_secrets_file(c_ln.logger, srv, configured_secrets)
            if can_proceed_s and can_proceed_ss and can_proceed_u:
                ac_lbl = crt_sequence['account-label']
                # variable with credentials for source server
                usr_dtl = configured_secrets[srv['vdr']][srv['typ']][srv['grp']][srv['lyr']][ac_lbl]
                can_proceed_uu = c_bnfe.validate_user_secrets(c_ln.logger, str_ss, usr_dtl)
            c_ln.logger.info(script_lcl.gettext('Validation of the 3 JSON files involved '
                             + '(Extraction Sequence, Source systems and User Secrets) '
                             + 'has been completed'))
        c_ln.logger.info(script_lcl.gettext('Preparing connection details has been completed'))
        t.stop()
        if can_proceed_e and can_proceed_s and can_proceed_ss and can_proceed_u and can_proceed_uu:
            server_vendor_and_type = srv['vdr'] + ' ' + srv['typ']
            if server_vendor_and_type in ('MariaDB Foundation MariaDB', 'Oracle MySQL', 'SAP HANA'):
                c_dbtkr.connect_to_database(c_ln.logger, t, {
                    'server-vendor-and-type': server_vendor_and_type,
                    'server-layer': srv['lyr'],
                    'ServerName': src_srvr['ServerName'],
                    'ServerPort': int(src_srvr['ServerPort']),
                    'Username': usr_dtl['Username'],
                    'Name': usr_dtl['Name'],
                    'Password': usr_dtl['Password'],
                })
            c_ln.logger.debug(script_lcl.gettext('Connection attempt done'))
            if c_dbtkr.conn is not None:
                # instantiate DB connection handler
                cursor = c_dbtkr.conn.cursor()
                for crt_query in crt_sequence['queries']:
                    t.start()
                    can_proceed_q = c_bnfe.validate_extraction_query(c_ln.logger, crt_query)
                    t.stop()
                    if can_proceed_q:
                        t.start()
                        initial_query = c_fo.fn_open_file_and_get_content(
                            crt_query['input-query-file'], 'raw')
                        fdbck = script_lcl.gettext('Generic query is: %s') \
                            .replace('%s', c_bn.fn_multi_line_string_to_single_line(initial_query))
                        c_ln.logger.info(fdbck)
                        t.stop()
                        for crt_session in crt_query['sessions']:
                            # conversion logic for legacy extraction sequence files - START
                            # this might be deprecated in the near future
                            if 'output-csv-file' in crt_session \
                                    and 'output-file' not in crt_session:
                                crt_session['output-file'] = {
                                    'format': 'csv',
                                    'name': crt_session['output-csv-file'],
                                }
                                if 'output-csv-separator' in crt_session:
                                    crt_session['output-file']['field-delimiter'] = \
                                        crt_session['output-csv-separator']
                            # conversion logic for legacy extraction sequence files - FINISH
                            # setting the start of the week as 1 which stands for Monday
                            if 'start-isoweekday' not in crt_session:
                                crt_session['start-isoweekday'] = 1
                                if 'start_isoweekday' in crt_session:
                                    crt_session['start-isoweekday'] = \
                                        crt_session['start_isoweekday']
                            wday_start = crt_session['start-isoweekday']
                            if 'parameters' in crt_session:
                                # assumption is for either DICT or LIST values are numeric
                                # in case text is given different rules have to be specified
                                if 'parameters-handling-rules' not in crt_session:
                                    crt_session['parameters-handling-rules'] = {
                                        "dict-values-glue"  : ", ",
                                        "dict-values-prefix": "IN (",
                                        "dict-values-suffix": ")",
                                        "list-values-glue"  : ", ",
                                        "list-values-prefix": "",
                                        "list-values-suffix": ""
                                    }
                            can_proceed_ses = c_bnfe.validate_query_session(c_ln.logger, str_ss,
                                                                            crt_session)
                            crt_session['output-file']['name'] = \
                                c_ph.eval_expression(c_ln.logger,
                                                     crt_session['output-file']['name'], wday_start)
                            extract_behaviour = c_bnfe.fn_set_extract_behaviour(crt_session)
                            resulted_file = crt_session['output-file']['name']
                            extraction_required = c_bnfe.fn_is_extraction_necessary(c_ln.logger, {
                                'extract-behaviour': extract_behaviour,
                                'output-csv-file': resulted_file,
                            })
                            if extract_behaviour == 'overwrite-if-output-file-exists' \
                                and 'extract-overwrite-condition' in crt_session \
                                    and Path(resulted_file).is_file():
                                fv = c_bnfe.fn_is_extraction_neccesary_additional(c_ln.logger,
                                                                                  c_ph, c_fo,
                                                                                  crt_session)
                                extraction_required = False
                                if fv == c_fo.lcl.gettext('older'):
                                    extraction_required = True
                            if extraction_required:
                                # get query parameters into a tuple
                                tuple_parameters = c_ph.handle_query_parameters(c_ln.logger,
                                                                                crt_session,
                                                                                wday_start)
                                # measure expected number of parameters
                                expected_number_of_parameters = str(initial_query).count('%s')
                                # simulate final query to log (useful for debugging purposes)
                                query = c_ph.simulate_final_query(c_ln.logger, t, initial_query,
                                                                  expected_number_of_parameters,
                                                                  tuple_parameters)
                                fdbck = script_lcl.gettext(\
                                    'Query with parameters interpreted is: %s') \
                                    .replace('%s', c_bn.fn_multi_line_string_to_single_line(query))
                                c_ln.logger.info(fdbck)
                                # actual execution of the query
                                cursor = c_dbtkr.execute_query(c_ln.logger, t, cursor,
                                                               initial_query,
                                                               expected_number_of_parameters,
                                                               tuple_parameters)
                                # bringing the information from server (data transfer)
                                result_set = c_dbtkr.fetch_executed_query(c_ln.logger, t, cursor)
                                # detecting the column named from result set
                                stats = {
                                    'columns': c_dbtkr.get_column_names(c_ln.logger, t, cursor),
                                    'rows_count': cursor.rowcount,
                                }
                                t.start()
                                c_ln.logger.info(script_lcl.gettext('Free DB result-set started'))
                                cursor.close()
                                c_ln.logger.info(script_lcl.gettext('Free DB result-set completed'))
                                t.stop()
                                if stats['rows_count'] > 0:
                                    # put result set into a data frame
                                    result_df = c_dbtkr.result_set_to_data_frame(c_ln.logger, t,
                                                                                 stats['columns'],
                                                                                 result_set)
                                    rdf = c_dbtkr.append_additional_columns_to_df(c_ln.logger, t,
                                                                                  result_df,
                                                                                  crt_session)
                                    c_dm.fn_store_data_frame_to_file(c_ln.logger, t, rdf,
                                                                     crt_session['output-file'])
                                    c_fo.fn_store_file_statistics(c_ln.logger, t, resulted_file,
                                                                  script_lcl.gettext( \
                                                                      'Output file name'))
                t.start()
                c_ln.logger.info(script_lcl.gettext('Closing DB connection'))
                c_dbtkr.conn.close()
                c_ln.logger.info(script_lcl.gettext('Closing DB completed'))
                t.stop()
            int_extracting_sequence += 1
    # just final message
    c_bn.fn_final_message(c_ln.logger, input_parameters.output_log_file,
                          t.timers.total(CURRENT_SCRIPT_NAME))
