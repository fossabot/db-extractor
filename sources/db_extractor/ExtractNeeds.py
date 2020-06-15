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
from db_extractor.BasicNeeds import BasicNeeds
from db_extractor.CommandLineArgumentsManagement import CommandLineArgumentsManagement
from db_extractor.DataInputOutput import DataInputOutput
from db_extractor.DataManipulator import DataManipulator
from db_extractor.FileOperations import FileOperations
from db_extractor.LoggingNeeds import LoggingNeeds
from db_extractor.ParameterHandling import ParameterHandling
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

    def __init__(self, destination_script, in_language='en_US'):
        self.script = destination_script
        file_parts = os.path.normpath(os.path.abspath(__file__)).replace('\\', os.path.altsep)\
            .split(os.path.altsep)
        locale_domain = file_parts[(len(file_parts)-1)].replace('.py', '')
        locale_folder = os.path.normpath(os.path.join(
            os.path.join(os.path.altsep.join(file_parts[:-2]), 'project_locale'), locale_domain))
        self.locale = gettext.translation(locale_domain, localedir=locale_folder,
                                          languages=[in_language], fallback=True)
        # instantiate Basic Needs class
        self.class_bn = BasicNeeds(in_language)
        # instantiate Extractor Specific Needs class
        self.class_bnfe = BasicNeedsForExtractor(in_language)
        # instantiate File Operations class
        self.class_fo = FileOperations(in_language)
        # instantiate File Operations class
        self.class_dbt = DatabaseTalker(in_language)
        # instantiate Data Manipulator class, useful to manipulate data frames
        self.class_dio = DataInputOutput(in_language)
        # instantiate Data Manipulator class, useful to manipulate data frames
        self.class_dm = DataManipulator(in_language)
        # instantiate Command Line Arguments class
        self.class_clam = CommandLineArgumentsManagement(in_language)
        # instantiate Logger class
        self.class_ln = LoggingNeeds()
        # instantiate Parameter Handling class
        self.class_ph = ParameterHandling(in_language)

    def build_dict_for_storage_file(self, crt_output):
        fn_dict = {
            'file list': crt_output['name'],
            'name': crt_output['name'],
            'format': crt_output['format'],
        }
        if 'compression' in crt_output:
            fn_dict['compression'] = crt_output['compression']
        if 'field delimiter' in crt_output:
            fn_dict['field delimiter'] = crt_output['field delimiter']
        return fn_dict

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
        simulated_query_single_line = self.class_bn.fn_multi_line_string_to_single(simulated_query)
        local_logger.info(self.locale.gettext('Query with parameters interpreted is: %s')
                          .replace('%s', simulated_query_single_line))
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
        self.class_fo.fn_store_file_statistics({
            'checksum included': self.parameters.include_checksum_in_files_statistics,
            'file list': self.parameters.input_extracting_sequence_file,
            'file meaning': self.locale.gettext(
                'Configuration file name with extracting sequence(es)'),
            'logger': self.class_ln.logger,
            'timer': self.timer,
        })
        # get the source system details from provided file
        self.timer.start()
        self.source_systems = self.class_fo.fn_open_file_and_get_content(
            self.parameters.input_source_system_file, 'json')['Systems']
        self.class_ln.logger.info(self.locale.gettext('Source Systems file name has been loaded'))
        self.timer.stop()
        self.class_fo.fn_store_file_statistics({
            'checksum included': self.parameters.include_checksum_in_files_statistics,
            'file list': self.parameters.input_source_system_file,
            'file meaning': self.locale.gettext('Source Systems file name'),
            'logger': self.class_ln.logger,
            'timer': self.timer,
        })
        # get the source system details from provided file
        self.timer.start()
        self.user_credentials = self.class_fo.fn_open_file_and_get_content(
            self.parameters.input_credentials_file, 'json')['Credentials']
        self.class_ln.logger.info(self.locale.gettext(
            'Configuration file name with credentials has been loaded'))
        self.timer.stop()
        self.class_fo.fn_store_file_statistics({
            'checksum included': self.parameters.include_checksum_in_files_statistics,
            'file list': self.parameters.input_credentials_file,
            'file meaning': self.locale.gettext('Configuration file name with credentials'),
            'logger': self.class_ln.logger,
            'timer': self.timer,
        })

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

    def result_set_to_disk_file(self, local_logger, stats, in_dict):
        result_df = self.class_dbt.result_set_to_data_frame(
            local_logger, self.timer, stats['columns'], stats['result_set'])
        if 'additional-columns' in in_dict['session']:
            if in_dict['session']['additional-columns'] == 'inherit-from-parent':
                in_dict['session']['additional-columns'] = in_dict['query']['additional-columns']
            elif in_dict['session']['additional-columns'] == 'inherit-from-grand-parent':
                in_dict['session']['additional-columns'] = in_dict['sequence']['additional-columns']
            result_df = self.class_dbt.append_additional_columns_to_df(
                local_logger, self.timer, result_df, in_dict['session'])
        self.store_result_set_to_disk(self.class_ln.logger, result_df, in_dict['session'])

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
        if output_file_setting_type in (dict, list):
            output_list = crt_session['output-file']
            if output_file_setting_type == dict:
                output_list = [crt_session['output-file']]
            for crt_output in output_list:
                fn_dict = self.build_dict_for_storage_file(crt_output)
                self.class_dio.fn_store_data_frame_to_file(
                    local_logger, self.timer, in_data_frame, fn_dict)
                self.class_fo.fn_store_file_statistics({
                    'checksum included': self.parameters.include_checksum_in_files_statistics,
                    'file list': crt_output['name'],
                    'file meaning': self.locale.gettext('Output file name'),
                    'logger': self.class_ln.logger,
                    'timer': self.timer,
                })
