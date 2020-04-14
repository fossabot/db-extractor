"""
Facilitates moving files from a specified directory and matching pattern to a destination directory
"""
# package to facilitate operating system operations
import os
# package to facilitate working with directories and files
from pathlib import Path
# useful methods to measure time performance by small pieces of code
from codetiming import Timer
# Custom classes specific to this package
from db_extractor.BasicNeeds import BasicNeeds
from db_extractor.CommandLineArgumentsManagement import CommandLineArgumentsManagement
from db_extractor.LoggingNeeds import LoggingNeeds
from db_extractor.DatabaseTalker import DatabaseTalker
from db_extractor.ParameterHandling import ParameterHandling
from db_extractor.DataManipulator import DataManipulator

# get current script name
current_script_name = os.path.basename(__file__).replace('.py', '')

# main execution logic
if __name__ == '__main__':
    # instantiate Basic Needs class
    c_bn = BasicNeeds()
    # load application configuration (inputs are defined into a json file)
    c_bn.fn_load_configuration()
    # instantiate Command Line Arguments class
    c_clam = CommandLineArgumentsManagement()
    parameters_in = c_clam.parse_arguments(c_bn.cfg_dtls['input_options']['db_extractor'])
    # checking inputs, if anything is invalid an exit(1) will take place
    c_bn.fn_check_inputs(parameters_in, current_script_name)
    # instantiate Logger class
    c_ln = LoggingNeeds()
    # initiate logger
    c_ln.initiate_logger(parameters_in.output_log_file, 'db_extractor')
    # define global timer to use
    t = Timer('db_extractor', text='Time spent is {seconds} ', logger=c_ln.logger.debug)
    # reflect title and input parameters given values in the log
    c_clam.listing_parameter_values(c_ln.logger, t, 'Data extractor',
                                    c_bn.cfg_dtls['input_options']['db_extractor'],
                                    parameters_in)
    # loading extracting sequence details
    t.start()
    extracting_sequences = c_bn.fn_open_file_and_get_content(
        parameters_in.input_extracting_sequence_file, 'json')
    c_ln.logger.info('Configuration file name with extracting sequence(es) has been loaded')
    t.stop()
    c_bn.fn_store_file_statistics(c_ln.logger, t, parameters_in.input_extracting_sequence_file,
                                  'Configuration file name with extracting sequence(es)')
    # get the source system details from provided file
    t.start()
    source_systems = c_bn.fn_open_file_and_get_content(
        parameters_in.input_source_system_file, 'json')['Systems']
    c_ln.logger.info('Source Systems file name has been loaded')
    t.stop()
    c_bn.fn_store_file_statistics(c_ln.logger, t, parameters_in.input_source_system_file,
                                  'SourceSystems file name')
    # get the source system details from provided file
    t.start()
    configured_secrets = c_bn.fn_open_file_and_get_content(
        parameters_in.input_credentials_file, 'json')['Credentials']
    c_ln.logger.info('Configuration file name with credentials has been loaded')
    t.stop()
    c_bn.fn_store_file_statistics(c_ln.logger, t, parameters_in.input_source_system_file,
                                  'SourceSystems file name')
    # instantiate Parameter Handling class
    c_ph = ParameterHandling()
    # instantiate Data Manipulator class, useful to manipulate data frames
    c_dm = DataManipulator()
    # instantiate Database Talker class
    c_dbtkr = DatabaseTalker()
    # cycling through the configurations
    for current_extracting_sequence in extracting_sequences:
        t.start()
        # just few values that's going to be used a lot
        srv = {
            'vdr': current_extracting_sequence['server-vendor'],
            'typ': current_extracting_sequence['server-type'],
            'grp': current_extracting_sequence['server-group'],
            'lyr': current_extracting_sequence['server-layer']
        }
        usr_lbl = current_extracting_sequence['account-label']
        # variable for source server details
        src_server = source_systems[srv['vdr']][srv['typ']]['Server'][srv['grp']][srv['lyr']]
        # variable with credentials for source server
        usr_dtl = configured_secrets[srv['vdr']][srv['typ']][srv['grp']][srv['lyr']][usr_lbl]
        c_ln.logger.info('Preparing connection details has been completed')
        t.stop()
        server_vendor_and_type = srv['vdr'] + ' ' + srv['typ']
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
            for current_query in current_extracting_sequence['queries']:
                t.start()
                initial_query = c_bn.fn_open_file_and_get_content(
                    current_query['input-query-file'], 'raw')
                c_ln.logger.info('Generic query is: '
                                 + c_bn.fn_multi_line_string_to_single_line(initial_query))
                t.stop()
                for current_session in current_query['sessions']:
                    current_session['output-csv-file'] = \
                        c_ph.special_case_string(c_ln.logger, current_session['output-csv-file'])
                    extraction_required = False
                    c_ln.logger.debug('Extract behaviour is set to '
                                      + current_session['extract-behaviour'])
                    if current_session['extract-behaviour'] == \
                            'skip-if-output-file-exists':
                        if Path(current_session['output-csv-file']).is_file():
                            c_ln.logger.debug('File ' + current_session['output-csv-file']
                                              + ' already exists, '
                                              + 'so database extraction will not be performed')
                        else:
                            extraction_required = True
                            c_ln.logger.debug('File ' + current_session['output-csv-file']
                                              + ' does not exist, '
                                              + 'so database extraction has to be performed')
                    elif current_session['extract-behaviour'] == \
                            'overwrite-if-output-file-exists':
                        extraction_required = True
                        c_ln.logger.debug('Database extraction has to be performed')
                    if extraction_required:
                        # ensure all potential query parameters are properly injected
                        query_to_run = c_ph.handle_query(c_ln.logger, t, current_session,
                                                         initial_query)
                        # actual execution of the query
                        cursor = c_dbtkr.execute_query(c_ln.logger, t, cursor, query_to_run)
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
                            result_data_frame = c_dbtkr.result_set_to_data_frame(c_ln.logger, t,
                                                                                 stats['columns'],
                                                                                 result_set)
                            rdf = c_dbtkr.append_additional_columns_to_data_frame(c_ln.logger, t,
                                                                                  result_data_frame,
                                                                                  current_session)
                            # store data frame to a specified output file
                            c_dm.fn_store_data_frame_to_file(c_ln.logger, t, rdf,
                                                             current_session['output-csv-file'],
                                                             current_session['output-csv-separator']
                                                             )
                            c_bn.fn_store_file_statistics(c_ln.logger, t,
                                                          current_session['output-csv-file'],
                                                          'Output file name')
            t.start()
            c_ln.logger.info('Closing DB connection')
            c_dbtkr.conn.close()
            c_ln.logger.info('Closing DB completed')
            t.stop()
    # just final message
    c_bn.fn_final_message(c_ln.logger, parameters_in.output_log_file,
                          t.timers.total('db_extractor'))
