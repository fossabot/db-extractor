"""
Facilitates moving files from a specified directory and matching pattern to a destination directory
"""
# package to facilitate operating system operations
import os
# package to facilitate working with directories and files
from pathlib import Path
# Custom classes specific to this package
from db_extractor.ExtractNeeds import ExtractNeeds
# get current script name
SCRIPT_NAME = os.path.basename(__file__).replace('.py', '')
SCRIPT_LANGUAGE = 'ro_RO'

# main execution logic
if __name__ == '__main__':
    # instantiate Logger class
    c_en = ExtractNeeds(SCRIPT_NAME, SCRIPT_LANGUAGE)
    # load script configuration
    c_en.load_configuration()
    # initiate Logging sequence
    c_en.initiate_logger_and_timer()
    # reflect title and input parameters given values in the log
    c_en.class_clam.listing_parameter_values(c_en.class_ln.logger, c_en.timer, 'Database Extractor',
                                             c_en.config['input_options'][SCRIPT_NAME],
                                             c_en.parameters)
    # loading extracting sequence details
    c_en.load_extraction_sequence_and_dependencies()
    # validation of the extraction sequence file
    c_en.class_bnfe.validate_extraction_sequence_file(c_en.class_ln.logger,
                                                      c_en.file_extract_sequence)
    # cycling through the configurations
    int_extracting_sequence = 1
    for crt_sequence in c_en.file_extract_sequence:
        c_en.timer.start()
        can_proceed_e = c_en.class_bnfe.validate_extraction_sequence(c_en.class_ln.logger,
                                                                     crt_sequence)
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
            can_proceed_s = c_en.class_bnfe.validate_source_systems_file(c_en.class_ln.logger, srv,
                                                                         c_en.source_systems)
            can_proceed_u = False
            if can_proceed_s:
                # variable for source server details
                src_srvr = c_en.source_systems[srv['vdr']][srv['typ']]['Server'][srv['grp']][srv['lyr']]
                str_ss = '"' + '", "'.join(srv.values()) + '"'
                can_proceed_ss = c_en.class_bnfe.validate_source_system(c_en.class_ln.logger, str_ss, src_srvr)
                can_proceed_u = c_en.class_bnfe.validate_user_secrets_file(c_en.class_ln.logger, srv,
                                                                           c_en.user_credentials)
            if can_proceed_s and can_proceed_ss and can_proceed_u:
                ac_lbl = crt_sequence['account-label']
                # variable with credentials for source server
                usr_dtl = c_en.user_credentials[srv['vdr']][srv['typ']][srv['grp']][srv['lyr']][ac_lbl]
                can_proceed_uu = c_en.class_bnfe.validate_user_secrets(c_en.class_ln.logger, str_ss, usr_dtl)
            c_en.class_ln.logger.info(c_en.locale.gettext('Validation of the 3 JSON files involved '
                                                          + '(Extraction Sequence, Source systems and User Secrets) '
                                                          + 'has been completed'))
        c_en.class_ln.logger.info(c_en.locale.gettext('Preparing connection details has been completed'))
        c_en.timer.stop()
        if can_proceed_e and can_proceed_s and can_proceed_ss and can_proceed_u and can_proceed_uu:
            server_vendor_and_type = srv['vdr'] + ' ' + srv['typ']
            if server_vendor_and_type in ('MariaDB Foundation MariaDB', 'Oracle MySQL', 'SAP HANA'):
                c_en.class_dbt.connect_to_database(c_en.class_ln.logger, c_en.timer, {
                    'server-vendor-and-type': server_vendor_and_type,
                    'server-layer': srv['lyr'],
                    'ServerName': src_srvr['ServerName'],
                    'ServerPort': int(src_srvr['ServerPort']),
                    'Username': usr_dtl['Username'],
                    'Name': usr_dtl['Name'],
                    'Password': usr_dtl['Password'],
                })
            c_en.class_ln.logger.debug(c_en.locale.gettext('Connection attempt done'))
            if c_en.class_dbt.conn is not None:
                # instantiate DB connection handler
                cursor = c_en.class_dbt.conn.cursor()
                for crt_query in crt_sequence['queries']:
                    c_en.timer.start()
                    can_proceed_q = c_en.class_bnfe.validate_extraction_query(c_en.class_ln.logger,
                                                                              crt_query)
                    c_en.timer.stop()
                    if can_proceed_q:
                        c_en.timer.start()
                        initial_query = c_en.class_fo.fn_open_file_and_get_content(
                            crt_query['input-query-file'], 'raw')
                        fdbck = c_en.locale.gettext('Generic query is: %s') \
                            .replace('%s',
                                     c_en.class_bn.fn_multi_line_string_to_single_line(initial_query))
                        c_en.class_ln.logger.info(fdbck)
                        c_en.timer.stop()
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
                            crt_session['start-isoweekday'] = \
                                c_en.set_default_starting_weekday(crt_session)
                            if 'parameters' in crt_session:
                                crt_session['parameters-handling-rules'] = \
                                    c_en.set_default_parameter_rules(crt_session)
                            can_proceed_ses = c_en.class_bnfe.validate_query_session(c_en.class_ln.logger, str_ss,
                                                                            crt_session)
                            extract_behaviour = c_en.class_bnfe.fn_set_extract_behaviour(crt_session)
                            if type(crt_session['output-file']) == dict:
                                crt_session['output-file']['name'] = \
                                    c_en.class_ph.eval_expression(c_en.class_ln.logger,
                                                                  crt_session['output-file']['name'],
                                                                  crt_session['start-isoweekday'])
                                resulted_file = crt_session['output-file']['name']
                                extraction_required = c_en.class_bnfe.fn_is_extraction_necessary(c_en.class_ln.logger, {
                                    'extract-behaviour': extract_behaviour,
                                    'output-csv-file': resulted_file,
                                })
                                if extract_behaviour == 'overwrite-if-output-file-exists' \
                                    and 'extract-overwrite-condition' in crt_session \
                                        and Path(resulted_file).is_file():
                                    fv = c_en.class_bnfe.fn_is_extraction_neccesary_additional(c_en.class_ln.logger, c_en.class_ph, c_en.class_fo, crt_session)
                                    extraction_required = False
                                    if fv == c_en.class_fo.lcl.gettext('older'):
                                        extraction_required = True
                            if can_proceed_ses and extraction_required:
                                dict_prepared = {
                                    'query': initial_query,
                                    'session': crt_session,
                                }
                                stats = c_en.extract_query_to_result_set(c_en.class_ln.logger,
                                                                         cursor, dict_prepared)
                                if stats['rows_counted'] > 0:
                                    rdf = c_en.result_set_into_data_frame(c_en.class_ln.logger,
                                                                          stats, crt_session)
                                    c_en.store_result_set_to_disk(c_en.class_ln.logger,
                                                                  rdf, crt_session)
                c_en.close_cursor(c_en.class_ln.logger, cursor)
            c_en.close_connection(c_en.class_ln.logger)
            int_extracting_sequence += 1
    # just final message
    c_en.class_bn.fn_final_message(c_en.class_ln.logger, c_en.parameters.output_log_file,
                                   c_en.timer.timers.total(SCRIPT_NAME))
