# standard Python packages
import datetime

# packages from pypi.org
from codetiming import Timer

# Custom classes specific to this package
from db_talker.BasicNeeds import BasicNeeds as ClassBN
from db_talker.CommandLineArgumentsManagement import CommandLineArgumentsManagement as ClassCLAM
from db_talker.DatabaseTalker import DatabaseTalker as ClassDT
from db_talker.LoggingNeeds import LoggingNeeds as ClassLN
from db_talker.ParameterHandling import ParameterHandling as ClassPH

const_time_uom = '[hours:minutes:seconds.miliseconds]'

# process expected parameters
parameters_interpreted = ClassCLAM.fn_db_talker_parse_arguments(ClassCLAM)
# initiate logger
ClassLN.initiate_logger(ClassLN, parameters_interpreted.output_log_file)
# define global timer to use
t = Timer('db_talker',
          text = 'Time spent is {seconds} ' + const_time_uom,
          logger = ClassLN.logger.debug
          )
# marking start of the Log
ClassLN.logger.info('='*50)
ClassLN.logger.info('DB Talker started')
# opening the configuration file specified as input
working_configuration_details = ClassBN.fn_load_working_configuration(ClassBN,
                                                                      ClassLN.logger,
                                                                      t,
                                                                      parameters_interpreted.
                                                                      input_configuration_file)
# load server configuration for this package
ClassBN.fn_load_server_configuration(ClassBN, ClassLN.logger, t)
# load user credentials
ClassBN.fn_load_credentials(ClassBN, ClassLN.logger, t)
# cycling through the configurations
for current_configuration_detail in working_configuration_details:
    t.start()
    # just few values that's going to be used a lot
    srv_grp = current_configuration_detail['server-group']
    # set a variable that's going to be used very frequently
    srv_lyr = current_configuration_detail['server-layer']
    # variable for HANA server details
    lst_hana_server = ClassBN.configured_details['Systems']['SAP']['HANA']['Server'][srv_grp][srv_lyr]
    # variable with credentials for HANA server
    usr_dtl = ClassBN.configured_secrets['Credentials']['SAP']['HANA'][srv_grp][srv_lyr]['Default']
    json_connection_details = {
        'server-layer'  : srv_lyr,
        'ServerName'    : lst_hana_server['ServerName'],
        'ServerPort'    : int(lst_hana_server['ServerPort']),
        'Username'      : usr_dtl['Username'],
        'Name'          : usr_dtl['Name'],
        'Password'      : usr_dtl['Password'],
    }
    t.stop()
    ClassDT.initiate_connection(ClassDT, ClassLN.logger, t, json_connection_details)
    for current_query in current_configuration_detail['queries']:
        # instantiate DB connection handler
        cursor = ClassDT.conn.cursor()
        # get generic query from provided file
        t.start()
        query_file = current_query['input-query-file']
        initial_query_to_run = ClassBN.fn_open_file_and_get_its_content(ClassBN, ClassLN.logger,
                                                                        query_file, 'raw')
        ClassLN.logger.info('Generic query is: ' + c_
                            + initial_query_to_run.replace('\n', ' ').replace('\r', '')
                            )
        t.stop()
        for current_session in current_query['sessions']:
            t.start()
            if 'parameters' in current_session:
                parameter_rules = []
                if 'parameters-handling-rules' in current_session:
                    parameter_rules = current_session['parameters-handling-rules']
                tp = ClassPH.build_parameters(ClassPH,
                                              ClassLN.logger,
                                              current_session['parameters'],
                                              parameter_rules,
                                              )
                try:
                    parameters_expected = initial_query_to_run.count('%s')
                    query_to_run = initial_query_to_run % tp
                    ClassLN.logger.info('Query with parameters interpreted is: '
                                        + query_to_run.replace('\n', ' ').replace('\r', '')
                                        )
                except TypeError as e:
                    ClassLN.logger.debug('Initial query expects ' + str(parameters_expected)
                                         + ' parameters but only ' + str(len(tp))
                                         + ' parameters were provided!')
                    ClassLN.logger.error(e)
            else:
                query_to_run = initial_query_to_run
            t.stop()
            query_session_details = {
                'cursor'            : cursor,
                'query'             : query_to_run,
                'session_details'   : current_session,
            }
            ClassDT.run_query_and_store_result(ClassDT, ClassLN.logger, t, query_session_details)
            if query_session_details['cursor'].getwarning() is None \
                    and query_session_details['cursor'].description is not None:
                result_set_content = ClassDT.fetch_executed_query(ClassDT, ClassLN.logger, t,
                                                                  query_session_details)
                column_names = ClassDT.determine_column_names(ClassDT, ClassLN.logger, t,
                                                              query_session_details['cursor'])
                packed_result_set = {
                    'csv-columns'       : column_names,
                    'output-csv-file'   : current_session['output-csv-file'],
                    'result-set'        : result_set_content,
                }
                if 'additional-columns' in current_session:
                    packed_result_set['additional-columns'] = current_session['additional-columns']
                ClassDT.store_resultset(ClassDT, ClassLN.logger, t, packed_result_set)
            else:
                ClassLN.logger.error(query_session_details['cursor'].haswarning())
        t.start()
        ClassLN.logger.info('Free DB result-set started')
        cursor.close()
        ClassLN.logger.info('Free DB result-set completed')
        t.stop()
    t.start()
    ClassLN.logger.info('Closing DB connection')
    ClassDT.conn.close()
    ClassLN.logger.info('Closing DB completed')
    t.stop()
# log the finish
ClassLN.logger.info('DB Talker finished')
ClassLN.logger.info(f'Total execution time was '
                    + str(datetime.timedelta(seconds = t.timers.total('db_talker')))
                    + ' ' + const_time_uom)
ClassLN.logger = None
print('~'*50 + 'DONE')
