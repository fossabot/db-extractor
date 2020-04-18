"""
Facilitates moving files from a specified directory and matching pattern to a destination directory
"""
# useful methods to measure time performance by small pieces of code
from codetiming import Timer
# package to facilitate operating system operations
import os
# common Custom classes
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

# main execution logic
if __name__ == '__main__':
    # instantiate Basic Needs class
    c_bn = BasicNeeds()
    # load application configuration (inputs are defined into a json file)
    crt_folder = os.path.dirname(__file__)
    configuration_file = os.path.join(crt_folder, 'config/db-extractor.json').replace('\\', '/')
    c_bn.fn_load_configuration(configuration_file)
    # instantiate Command Line Arguments class
    c_clam = CommandLineArgumentsManagement()
    # get command line parameter values
    in_params = c_clam.parse_arguments(c_bn.cfg_dtls['input_options']['db_extractor'])
    # checking inputs, if anything is invalid an exit(1) will take place
    c_bn.fn_check_inputs(in_params)
    # instantiate Extractor Specific Needs class
    c_bnfe = BasicNeedsForExtractor()
    # checking inputs, if anything is invalid an exit(1) will take place
    c_bnfe.fn_check_inputs_specific(in_params)
    # instantiate Logger class
    c_ln = LoggingNeeds()
    # initiate logger
    c_ln.initiate_logger(in_params.output_log_file, 'db_extractor')
    # define global timer to use
    t = Timer('db_extractor', text='Time spent is {seconds} ', logger=c_ln.logger.debug)
    # reflect title and input parameters given values in the log
    c_clam.listing_parameter_values(c_ln.logger, t, 'Data extractor',
                                    c_bn.cfg_dtls['input_options']['db_extractor'],
                                    in_params)
    # loading extracting sequence details
    t.start()
    extracting_sequences = c_bn.fn_open_file_and_get_content(
        in_params.input_extracting_sequence_file, 'json')
    c_ln.logger.info('Extraction sequence file name with extracting sequence(es) has been loaded')
    t.stop()
    c_bn.fn_store_file_statistics(c_ln.logger, t, in_params.input_extracting_sequence_file,
                                  'Configuration file name with extracting sequence(es)')
    # validation of the extraction sequence file
    file_is_valid = c_bnfe.validate_extraction_sequence_file(c_ln.logger, extracting_sequences)
    if not file_is_valid:
        exit(1)
    # get the source system details from provided file
    t.start()
    source_systems = c_bn.fn_open_file_and_get_content(
        in_params.input_source_system_file, 'json')['Systems']
    c_ln.logger.info('Source Systems file name has been loaded')
    t.stop()
    c_bn.fn_store_file_statistics(c_ln.logger, t, in_params.input_source_system_file,
                                  'SourceSystems file name')
    # get the source system details from provided file
    t.start()
    configured_secrets = c_bn.fn_open_file_and_get_content(
        in_params.input_credentials_file, 'json')['Credentials']
    c_ln.logger.info('Configuration file name with credentials has been loaded')
    t.stop()
    c_bn.fn_store_file_statistics(c_ln.logger, t, in_params.input_source_system_file,
                                  'SourceSystems file name')
    # instantiate Parameter Handling class
    c_ph = ParameterHandling()
    # instantiate Data Manipulator class, useful to manipulate data frames
    c_dm = DataManipulator()
    # instantiate Database Talker class
    c_dbtkr = DatabaseTalker()
    # cycling through the configurations
    int_extracting_sequence = 1
    for crt_sequence in extracting_sequences:
        t.start()
        can_proceed_e = c_bnfe.validate_extraction_sequence(c_ln.logger, crt_sequence)
        can_proceed_s = c_bnfe.validate_source_systems_file(c_ln.logger, crt_sequence,
                                                            source_systems)
        can_proceed_ss = False
        if can_proceed_e:
            # just few values that's going to be used a lot
            srv = {
                'vdr': crt_sequence['server-vendor'],
                'typ': crt_sequence['server-type'],
                'grp': crt_sequence['server-group'],
                'lyr': crt_sequence['server-layer']
            }
            # variable for source server details
            src_server = source_systems[srv['vdr']][srv['typ']]['Server'][srv['grp']][srv['lyr']]
            str_ss = '"' + '", "'.join(srv.values()) + '"'
            can_proceed_ss = c_bnfe.validate_source_system(c_ln.logger, str_ss, src_server)
        c_ln.logger.info('Validation of the 3 JSON files involved '
                         + '(Extraction Sequence, Source systems and User Secrets) '
                         + 'has been completed')
        t.stop()
        if can_proceed_e and can_proceed_s and can_proceed_ss:
            t.start()
            usr_lbl = crt_sequence['account-label']
            # variable with credentials for source server
            usr_dtl = configured_secrets[srv['vdr']][srv['typ']][srv['grp']][srv['lyr']][usr_lbl]
            server_vendor_and_type = srv['vdr'] + ' ' + srv['typ']
            c_ln.logger.info('Preparing connection details has been completed')
            t.stop()
            if server_vendor_and_type in ('Oracle MySQL', 'SAP HANA'):
                c_dbtkr.connect_to_database(c_ln.logger, t, {
                    'server-vendor-and-type': server_vendor_and_type,
                    'server-layer': srv['lyr'],
                    'ServerName': src_server['ServerName'],
                    'ServerPort': int(src_server['ServerPort']),
                    'Username': usr_dtl['Username'],
                    'Name': usr_dtl['Name'],
                    'Password': usr_dtl['Password'],
                })
            c_ln.logger.debug('Connection attempt done')
            if c_dbtkr.conn is not None:
                # instantiate DB connection handler
                cursor = c_dbtkr.conn.cursor()
                for crt_query in crt_sequence['queries']:
                    t.start()
                    initial_query = c_bn.fn_open_file_and_get_content(
                        crt_query['input-query-file'], 'raw')
                    c_ln.logger.info('Generic query is: '
                                     + c_bn.fn_multi_line_string_to_single_line(initial_query))
                    t.stop()
                    for crt_session in crt_query['sessions']:
                        crt_session['output-csv-file'] = \
                            c_ph.special_case_string(c_ln.logger,
                                                     crt_session['output-csv-file'])
                        extract_behaviour = c_bnfe.fn_set_extract_behaviour(crt_session)
                        extraction_required = c_bnfe.fn_is_extraction_necessary(c_ln.logger, {
                            'extract-behaviour': extract_behaviour,
                            'output-csv-file': crt_session['output-csv-file'],
                        })
                        if extraction_required:
                            # get query parameters into a tuple
                            tuple_parameters = c_ph.handle_query_parameters(c_ln.logger,
                                                                            crt_session)
                            # measure expected number of parameters
                            parameters_expected = initial_query.count('%s')
                            # simulate final query to log (useful for debugging purposes)
                            query = c_ph.simulate_final_query(c_ln.logger, t, initial_query,
                                                              parameters_expected,
                                                              tuple_parameters)
                            c_ln.logger.info('Query with parameters interpreted is: '
                                             + c_bn.fn_multi_line_string_to_single_line(query))
                            # actual execution of the query
                            cursor = c_dbtkr.execute_query(c_ln.logger, t, cursor, initial_query,
                                                           parameters_expected, tuple_parameters)
                            # bringing the information from server (data transfer)
                            result_set = c_dbtkr.fetch_executed_query(c_ln.logger, t, cursor)
                            # detecting the column named from result set
                            stats = {
                                'columns': c_dbtkr.get_column_names(c_ln.logger, t, cursor),
                                'rows_count': cursor.rowcount,
                            }
                            t.start()
                            c_ln.logger.info('Free DB result-set started')
                            cursor.close()
                            c_ln.logger.info('Free DB result-set completed')
                            t.stop()
                            if stats['rows_count'] > 0:
                                # put result set into a data frame
                                result_df = c_dbtkr.result_set_to_data_frame(c_ln.logger, t,
                                                                             stats['columns'],
                                                                             result_set)
                                rdf = c_dbtkr.append_additional_columns_to_data_frame(c_ln.logger,
                                                                                      t,
                                                                                      result_df,
                                                                                      crt_session)
                                # store data frame to a specified output file
                                c_dm.fn_store_data_frame_to_file(c_ln.logger, t, rdf,
                                                                 crt_session['output-csv-file'],
                                                                 crt_session['output-csv-separator']
                                                                 )
                                c_bn.fn_store_file_statistics(c_ln.logger, t,
                                                              crt_session['output-csv-file'],
                                                              'Output file name')
                t.start()
                c_ln.logger.info('Closing DB connection')
                c_dbtkr.conn.close()
                c_ln.logger.info('Closing DB completed')
                t.stop()
            int_extracting_sequence += 1
    # just final message
    c_bn.fn_final_message(c_ln.logger, in_params.output_log_file,
                          t.timers.total('db_extractor'))
