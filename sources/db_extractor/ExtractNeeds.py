"""
main class to support Extract script
"""
# useful methods to measure time performance by small pieces of code
from codetiming import Timer
# package to facilitate operating system operations
import os
# package to add support for multi-language (i18n)
import gettext
# package to facilitate working with directories and files
from pathlib import Path
# custom classes specific to this project
from common.BasicNeeds import BasicNeeds
from common.CommandLineArgumentsManagement import CommandLineArgumentsManagement
from common.DataInputOutput import DataInputOutput
from common.DataManipulator import DataManipulator
from common.FileOperations import FileOperations
from common.LoggingNeeds import LoggingNeeds
from common.ParameterHandling import ParameterHandling
from db_extractor.BasicNeedsForExtractor import BasicNeedsForExtractor
from db_extractor.DatabaseTalker import DatabaseTalker


class ExtractNeeds:
    class_bn = None
    class_bnfe = None
    class_clam = None
    class_dbt = None
    class_dio = None
    class_dm = None
    class_fo = None
    class_ln = None
    class_ph = None
    config = None
    file_extract_sequence = None
    locale = None
    parameters = None
    script = None
    source_systems = None
    timer = None
    user_credentials = None

    def __init__(self, destination_script, default_language='en_US'):
        self.script = destination_script
        current_file_basename = os.path.basename(__file__).replace('.py', '')
        lang_folder = os.path.join(os.path.dirname(__file__), current_file_basename + '_Locale')
        self.locale = gettext.translation(
                current_file_basename, lang_folder, languages=[default_language])
        # instantiate Basic Needs class
        self.class_bn = BasicNeeds(default_language)
        # instantiate Extractor Specific Needs class
        self.class_bnfe = BasicNeedsForExtractor(default_language)
        # instantiate File Operations class
        self.class_fo = FileOperations(default_language)
        # instantiate File Operations class
        self.class_dbt = DatabaseTalker(default_language)
        # instantiate Data Manipulator class, useful to manipulate data frames
        self.class_dio = DataInputOutput(default_language)
        # instantiate Data Manipulator class, useful to manipulate data frames
        self.class_dm = DataManipulator(default_language)
        # instantiate Command Line Arguments class
        self.class_clam = CommandLineArgumentsManagement(default_language)
        # instantiate Logger class
        self.class_ln = LoggingNeeds()
        # instantiate Parameter Handling class
        self.class_ph = ParameterHandling(default_language)

    def close_connection(self, local_logger):
        self.timer.start()
        local_logger.info(self.locale.gettext('Closing DB connection'))
        self.class_dbt.connection.close()
        local_logger.info(self.locale.gettext('Closing DB completed'))
        self.timer.stop()

    def close_cursor(self, local_logger, in_cursor):
        self.timer.start()
        local_logger.info(self.locale.gettext('Free DB result-set started'))
        in_cursor.close()
        local_logger.info(self.locale.gettext('Free DB result-set completed'))
        self.timer.stop()

    def evaluate_extraction_overwrite_condition(self, extraction_required, in_dict):
        if in_dict['session']['extract-behaviour'] == 'overwrite-if-output-file-exists' \
                and 'extract-overwrite-condition' in in_dict['session'] \
                and Path(in_dict['file']['name']).is_file():
            fv = self.class_bnfe.fn_is_extraction_necessary_additional(
                self.class_ln.logger, self.class_ph, self.class_fo, in_dict)
            extraction_required = False
            new_verdict = self.locale.gettext('not required')
            if fv == self.class_fo.locale.gettext('older'):
                extraction_required = True
                new_verdict = self.locale.gettext('required')
            self.class_ln.logger.debug(self.locale.gettext(
                'Additional evaluation took place and new verdict is: {new_verdict}')
                                       .replace('{new_verdict}', new_verdict))
        return extraction_required

    def evaluate_if_extraction_is_required(self, in_dict):
        extraction_required = False
        if type(in_dict['session']['output-file']) == dict:
            extraction_required = self.evaluate_if_extraction_is_required_for_single_file({
                'session': in_dict['session'],
                'query': in_dict['query'],
                'sequence': in_dict['sequence'],
                'file': in_dict['session']['output-file'],
            })
        elif type(in_dict['session']['output-file']) == list:
            extraction_required = self.evaluate_if_extraction_is_required_list(in_dict)
        return extraction_required

    def evaluate_if_extraction_is_required_list(self, in_dict):
        evaluated_extraction = {}
        for crt_file in in_dict['session']['output-file']:
            crt_eval = self.evaluate_if_extraction_is_required_for_single_file({
                'session': in_dict['session'],
                'query': in_dict['query'],
                'sequence': in_dict['sequence'],
                'file': crt_file,
            })
            evaluated_extraction.update({str(crt_file['name']): crt_eval})
        extraction_required = self.class_bn.fn_evaluate_dict_values(evaluated_extraction)
        self.class_ln.logger.debug(evaluated_extraction)
        overall_verdict = self.locale.gettext('not required')
        if extraction_required:
            overall_verdict = self.locale.gettext('required')
        self.class_ln.logger.debug(self.locale.gettext(
            'Overall new verdict after considering multiple files is: {overall_verdict}')
                                   .replace('{overall_verdict}', overall_verdict))
        return extraction_required

    def evaluate_if_extraction_is_required_for_single_file(self, in_dict):
        in_dict['file']['name'] = self.class_ph.eval_expression(
            self.class_ln.logger, in_dict['file']['name'], in_dict['session']['start-iso-weekday'])
        e_dict = {
            'extract-behaviour': in_dict['session']['extract-behaviour'],
            'output-csv-file': in_dict['file']['name'],
        }
        extraction_required = self.class_bnfe.fn_is_extraction_necessary(
            self.class_ln.logger, e_dict)
        extraction_required = self.evaluate_extraction_overwrite_condition(
            extraction_required, in_dict)
        return extraction_required

    def extract_query_to_result_set(self, local_logger, in_cursor, in_dictionary):
        this_session = in_dictionary['session']
        this_query = in_dictionary['query']
        # get query parameters into a tuple
        tuple_parameters = self.class_ph.handle_query_parameters(
                local_logger, this_session, this_session['start-iso-weekday'])
        # measure expected number of parameters
        expected_no_of_parameters = str(this_query).count('%s')
        # simulate final query to log (useful for debugging purposes)
        simulated_query = self.class_ph.simulate_final_query(
                local_logger, self.timer, this_query, expected_no_of_parameters, tuple_parameters)
        local_logger.info(self.locale.gettext('Query with parameters interpreted is: %s')
                          .replace('%s',
                                   self.class_bn.fn_multi_line_string_to_single(simulated_query)))
        # actual execution of the query
        in_cursor = self.class_dbt.execute_query(
                local_logger, self.timer, in_cursor, this_query,
                expected_no_of_parameters, tuple_parameters)
        # bringing the information from server (data transfer)
        dict_to_return = {
            'rows_counted': 0
        }
        if in_cursor is not None:
            dict_to_return = {
                'columns': self.class_dbt.get_column_names(local_logger, self.timer, in_cursor),
                'result_set': self.class_dbt.fetch_executed_query(
                        local_logger, self.timer, in_cursor),
                'rows_counted': in_cursor.rowcount,
            }
        return dict_to_return

    def initiate_logger_and_timer(self):
        # initiate logger
        self.class_ln.initiate_logger(self.parameters.output_log_file, self.script)
        # initiate localization specific for this script
        # define global timer to use
        self.timer = Timer(self.script,
                           text=self.locale.gettext('Time spent is {seconds}'),
                           logger=self.class_ln.logger.debug)

    def load_configuration(self):
        # load application configuration (inputs are defined into a json file)
        ref_folder = os.path.dirname(__file__).replace('db_extractor', 'config')
        config_file = os.path.join(ref_folder, 'db-extractor.json').replace('\\', '/')
        self.config = self.class_fo.fn_open_file_and_get_content(config_file)
        # get command line parameter values
        self.parameters = self.class_clam.parse_arguments(self.config['input_options'][self.script])
        # checking inputs, if anything is invalid an exit(1) will take place
        self.class_bn.fn_check_inputs(self.parameters)
        # checking inputs, if anything is invalid an exit(1) will take place
        self.class_bnfe.fn_check_inputs_specific(self.parameters)

    def load_extraction_sequence_and_dependencies(self):
        self.timer.start()
        self.file_extract_sequence = self.class_fo.fn_open_file_and_get_content(
            self.parameters.input_extracting_sequence_file, 'json')
        self.class_ln.logger.info(self.locale.gettext(
            'Configuration file name with extracting sequence(es) has been loaded'))
        self.timer.stop()
        # store file statistics
        self.class_fo.fn_store_file_statistics(
                self.class_ln.logger, self.timer, self.parameters.input_extracting_sequence_file,
                self.locale.gettext('Configuration file name with extracting sequence(es)'))
        # get the source system details from provided file
        self.timer.start()
        self.source_systems = self.class_fo.fn_open_file_and_get_content(
            self.parameters.input_source_system_file, 'json')['Systems']
        self.class_ln.logger.info(self.locale.gettext('Source Systems file name has been loaded'))
        self.timer.stop()
        self.class_fo.fn_store_file_statistics(
                self.class_ln.logger, self.timer, self.parameters.input_source_system_file,
                self.locale.gettext('Source Systems file name'))
        # get the source system details from provided file
        self.timer.start()
        self.user_credentials = self.class_fo.fn_open_file_and_get_content(
            self.parameters.input_credentials_file, 'json')['Credentials']
        self.class_ln.logger.info(self.locale.gettext(
            'Configuration file name with credentials has been loaded'))
        self.timer.stop()
        self.class_fo.fn_store_file_statistics(
                self.class_ln.logger, self.timer, self.parameters.input_credentials_file,
                self.locale.gettext('Configuration file name with credentials'))

    def load_query(self, crt_query):
        self.timer.start()
        query = self.class_fo.fn_open_file_and_get_content(crt_query['input-query-file'], 'raw')
        feedback = self.locale.gettext('Generic query is: %s') \
            .replace('%s', self.class_bn.fn_multi_line_string_to_single(query))
        self.class_ln.logger.info(feedback)
        self.timer.stop()
        return query

    @staticmethod
    def pack_three_levels(in_session, in_query, in_sequence):
        return {
            'session': in_session,
            'query': in_query,
            'sequence': in_sequence,
        }

    def result_set_into_data_frame(self, local_logger, stats, in_dict):
        result_df = self.class_dbt.result_set_to_data_frame(
            local_logger, self.timer, stats['columns'], stats['result_set'])
        if 'additional-columns' in in_dict['session']:
            if in_dict['session']['additional-columns'] == 'inherit-from-parent':
                in_dict['session']['additional-columns'] = in_dict['query']['additional-columns']
            elif in_dict['session']['additional-columns'] == 'inherit-from-grand-parent':
                in_dict['session']['additional-columns'] = in_dict['sequence']['additional-columns']
            result_df = self.class_dbt.append_additional_columns_to_df(
                local_logger, self.timer, result_df, in_dict['session'])
        return result_df

    @staticmethod
    def set_default_starting_weekday(in_dict):
        week_starts_with_iso_weekday = 1
        if 'start-iso-weekday' in in_dict['session']:
            if in_dict['session']['start-iso-weekday'] == 'inherit-from-parent':
                in_dict['session']['start-iso-weekday'] = in_dict['query']['start-iso-weekday']
            elif in_dict['session']['start-iso-weekday'] == 'inherit-from-grand-parent':
                in_dict['session']['start-iso-weekday'] = in_dict['sequence']['start-iso-weekday']
            week_starts_with_iso_weekday = in_dict['session']['start-iso-weekday']
        return week_starts_with_iso_weekday

    @staticmethod
    def set_default_parameter_rules(in_dict):
        # assumption is for either DICT or LIST values are numeric
        # in case text is given different rules have to be specified
        dictionary_to_return = {
            "dict-values-glue": ", ",
            "dict-values-prefix": "IN (",
            "dict-values-suffix": ")",
            "list-values-glue": ", ",
            "list-values-prefix": "",
            "list-values-suffix": ""
        }
        if 'parameters-handling-rules' in in_dict['session']:
            dictionary_to_return = in_dict['session']['parameters-handling-rules']
            if dictionary_to_return == 'inherit-from-parent':
                dictionary_to_return = in_dict['query']['parameters-handling-rules']
            elif dictionary_to_return == 'inherit-from-grand-parent':
                dictionary_to_return = in_dict['sequence']['parameters-handling-rules']
        return dictionary_to_return

    def store_result_set_to_disk(self, local_logger, in_data_frame, crt_session):
        output_file_setting_type = type(crt_session['output-file'])
        if output_file_setting_type == dict:
            self.class_dio.fn_store_data_frame_to_file(
                    local_logger, self.timer, in_data_frame, crt_session['output-file'])
            self.class_fo.fn_store_file_statistics(
                    local_logger, self.timer, crt_session['output-file']['name'],
                    self.locale.gettext('Output file name'))
        elif output_file_setting_type == list:
            for crt_output in crt_session['output-file']:
                self.class_dio.fn_store_data_frame_to_file(
                        local_logger, self.timer, in_data_frame, crt_output)
                self.class_fo.fn_store_file_statistics(
                        local_logger, self.timer, crt_output['name'],
                        self.locale.gettext('Output file name'))
