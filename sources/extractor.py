"""
Facilitates moving files from a specified directory and matching pattern to a destination directory
"""
# package to facilitate operating system operations
import os
# useful methods to measure time performance by small pieces of code
from codetiming import Timer
# Custom classes specific to this package
from sources.data_extractor.BasicNeeds import BasicNeeds
from sources.data_extractor.CommandLineArgumentsManagement import CommandLineArgumentsManagement
from sources.data_extractor.LoggingNeeds import LoggingNeeds
from sources.data_extractor.DatabaseTalker import DatabaseTalker
from sources.data_extractor.ParameterHandling import ParameterHandling
from sources.data_extractor.DataManipulator import DataManipulator

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
    parameters_in = c_clam.parse_arguments(c_bn.cfg_dtls['input_options']['data_extractor'])
    # checking inputs, if anything is invalid an exit(1) will take place
    c_bn.fn_check_inputs(parameters_in, current_script_name)
    # instantiate Logger class
    c_ln = LoggingNeeds()
    # initiate logger
    c_ln.initiate_logger(parameters_in.output_log_file, 'data_extractor')
    # define global timer to use
    t = Timer('data_extractor', text='Time spent is {seconds} ', logger=c_ln.logger.debug)
    # reflect title and input parameters given values in the log
    c_clam.listing_parameter_values(c_ln.logger, t, 'Data extractor',
                                    c_bn.cfg_dtls['input_options']['data_extractor'],
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
        # variable for source server details
        src_server = source_systems[srv['vdr']][srv['typ']]['Server'][srv['grp']][srv['lyr']]
        # variable with credentials for source server
        usr_dtl = configured_secrets[srv['vdr']][srv['typ']][srv['grp']][srv['lyr']]['Default']
        c_ln.logger.info('Preparing connection details has been completed')
        t.stop()
        # instantiate Database Talker class
        c_dbtkr = DatabaseTalker()
        '''
        if srv['vdr'] == 'SAP' and srv['typ'] == 'HANA':
            c_dbtkr.connect_to_sap_hana(c_ln.logger, t, {
                'server-layer': srv['lyr'],
                'ServerName': src_server['ServerName'],
                'ServerPort': int(src_server['ServerPort']),
                'Username': usr_dtl['Username'],
                'Name': usr_dtl['Name'],
                'Password': usr_dtl['Password'],
             })
        # instantiate DB connection handler
        cursor = c_dbtkr.conn.cursor()
        '''
        for current_query in current_extracting_sequence['queries']:
            t.start()
            initial_query = c_bn.fn_open_file_and_get_content(
                current_query['input-query-file'], 'raw')
            c_ln.logger.info('Generic query is: '
                             + c_bn.fn_multi_line_string_to_single_line(initial_query))
            t.stop()
            for current_session in current_query['sessions']:
                t.start()
                if 'parameters' in current_session:
                    parameter_rules = []
                    if 'parameters-handling-rules' in current_session:
                        parameter_rules = current_session['parameters-handling-rules']
                    tp = c_ph.build_parameters(c_ln.logger, current_session['parameters'],
                                               parameter_rules)
                    try:
                        parameters_expected = initial_query.count('%s')
                        query_to_run = initial_query % tp
                        c_ln.logger.info('Query with parameters interpreted is: '
                                         + c_bn.fn_multi_line_string_to_single_line(query_to_run))
                    except TypeError as e:
                        c_ln.logger.debug('Initial query expects ' + str(parameters_expected)
                                          + ' parameters but only ' + str(len(tp))
                                          + ' parameters were provided!')
                        c_ln.logger.error(e)
                else:
                    query_to_run = initial_query
                t.stop()
                '''
                t.start()
                c_ln.logger.info('Free DB result-set started')
                cursor.close()
                c_ln.logger.info('Free DB result-set completed')
                t.stop()
                '''
            # instantiate Basic Needs class
            # c_dm = DataManipulator()
        '''
        t.start()
        c_ln.logger.info('Closing DB connection')
        c_dbtkr.conn.close()
        c_ln.logger.info('Closing DB completed')
        t.stop()
        '''
    # just final message
    c_bn.fn_final_message(c_ln.logger, parameters_in.output_log_file,
                          t.timers.total('data_extractor'))
