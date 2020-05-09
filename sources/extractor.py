"""
Facilitates moving files from a specified directory and matching pattern to a destination directory
"""
# package to facilitate operating system locale detection
import locale
# package to facilitate operating system operations
import os
# package to facilitate multiple operation system operations
import platform

# Custom classes specific to this package
from project_locale.localizations_common import LocalizationsCommon
from db_extractor.ExtractNeeds import ExtractNeeds

# get current script name
SCRIPT_NAME = os.path.basename(__file__).replace('.py', '')

# main execution logic
if __name__ == '__main__':
    # instantiate Localizations Common class
    class_lc = LocalizationsCommon()
    # ensure all compiled localization files are in place (as needed for localized messages later)
    class_lc.run_localization_compile()
    # establish localization language to use
    language_to_use = class_lc.get_region_language_to_use_from_operating_system()
    # instantiate Logger class
    c_en = ExtractNeeds(SCRIPT_NAME, language_to_use)
    # load script configuration
    c_en.load_configuration()
    # initiate Logging sequence
    c_en.initiate_logger_and_timer()
    # reflect title and input parameters given values in the log
    c_en.class_clam.listing_parameter_values(
        c_en.class_ln.logger, c_en.timer, 'Database Extractor',
        c_en.config['input_options'][SCRIPT_NAME], c_en.parameters)
    # loading extracting sequence details
    c_en.load_extraction_sequence_and_dependencies()
    # validation of the extraction sequence file
    if c_en.class_bnfe.validate_all_json_files(
            c_en.class_ln.logger, c_en.timer, c_en.file_extract_sequence):
        # cycling through the configurations
        for seq_idx, crt_sequence in enumerate(c_en.file_extract_sequence):
            can_proceed = \
                c_en.class_bnfe.validate_all_json_files_current(
                    c_en.class_ln.logger, c_en.timer, crt_sequence, seq_idx,
                    c_en.source_systems, c_en.user_credentials)
            if c_en.class_bn.fn_evaluate_dict_values(can_proceed):
                c_en.class_dbt.connect_to_database(
                    c_en.class_ln.logger, c_en.timer, c_en.class_bnfe.connection_details)
                if c_en.class_dbt.connection is not None:
                    # instantiate DB connection handler
                    cursor = c_en.class_dbt.connection.cursor()
                    for crt_query in crt_sequence['queries']:
                        can_proceed_q = \
                            c_en.class_bnfe.validate_extraction_query(
                                c_en.class_ln.logger, c_en.timer, crt_query)
                        if can_proceed_q:
                            the_query = c_en.load_query(crt_query)
                            for crt_session in crt_query['sessions']:
                                dict__child__parent__grand_parent = c_en.pack_three_levels(
                                    crt_session, crt_query, crt_sequence)
                                crt_session['start-iso-weekday'] = \
                                    c_en.set_default_starting_weekday(
                                        dict__child__parent__grand_parent)
                                dict__child__parent__grand_parent = c_en.pack_three_levels(
                                    crt_session, crt_query, crt_sequence)
                                if 'parameters' in crt_session:
                                    crt_session['parameters-handling-rules'] = \
                                        c_en.set_default_parameter_rules(
                                            dict__child__parent__grand_parent)
                                can_proceed_ses = \
                                    c_en.class_bnfe.validate_query_session(
                                        c_en.class_ln.logger, crt_session)
                                crt_session['extract-behaviour'] = \
                                    c_en.class_bnfe.fn_set_extract_behaviour(crt_session)
                                dict__child__parent__grand_parent = c_en.pack_three_levels(
                                    crt_session, crt_query, crt_sequence)
                                extraction_required = c_en.evaluate_if_extraction_is_required(
                                    dict__child__parent__grand_parent)
                                if can_proceed_ses and extraction_required:
                                    dict_prepared = {
                                        'query': the_query,
                                        'session': crt_session,
                                    }
                                    stats = c_en.extract_query_to_result_set(
                                        c_en.class_ln.logger, cursor, dict_prepared)
                                    if stats['rows_counted'] > 0:
                                        dict__child__parent__grand_parent = c_en.pack_three_levels(
                                            crt_session, crt_query, crt_sequence)
                                        c_en.result_set_to_disk_file(
                                            c_en.class_ln.logger, stats,
                                            dict__child__parent__grand_parent)
                        c_en.close_cursor(c_en.class_ln.logger, cursor)
                    c_en.close_connection(c_en.class_ln.logger)
    # just final message
    c_en.class_bn.fn_final_message(c_en.class_ln.logger, c_en.parameters.output_log_file,
                                   c_en.timer.timers.total(SCRIPT_NAME))
