"""
Data Input Output class
"""
# package to add support for multi-language (i18n)
import gettext
# package to handle files/folders and related metadata/operations
import os
# package facilitating Data Frames manipulation
import pandas


class DataInputOutput:
    locale = None

    def __init__(self, default_language='en_US'):
        current_script = os.path.basename(__file__).replace('.py', '')
        lang_folder = os.path.join(os.path.dirname(__file__), current_script + '_Locale')
        self.locale = gettext.translation(current_script, lang_folder, languages=[default_language])

    def fn_build_feedback_for_logger(self, operation_details):
        messages = {}
        if operation_details['operation'] == 'load':
            files_counted = str(operation_details['files counted'])
            messages = {
                'failed': self.locale.gettext('Error encountered on loading Pandas Data Frame '
                                              + 'from {file_type} file type (see below)'),
                'success': self.locale.gettext(
                    'All {files_counted} files of type {file_type} '
                    + 'successfully added to a Pandas Data Frame').replace('{files_counted}',
                                                                           files_counted)
            }
        elif operation_details['operation'] == 'save':
            messages = {
                'failed': self.locale.gettext('Error encountered on saving Pandas Data Frame '
                                              + 'into a {file_type} file type (see below)'),
                'success': self.locale.gettext(
                    'Pandas Data Frame has just been saved to file "{file_name}", '
                    + 'considering {file_type} as file type').replace('{file_name}',
                                                                      operation_details['name']),
            }
        messages['failed'].replace('{file_type}',  operation_details['format'].upper())
        messages['success'].replace('{file_type}',  operation_details['format'].upper())
        return messages

    @staticmethod
    def fn_default_load_dict_message(in_file_list, in_format):
        return {
            'error details': None,
            'files counted': len(in_file_list),
            'format': in_format,
            'operation': 'load',
        }

    def fn_file_operation_logger(self, local_logger, in_logger_dict):
        messages = self.fn_build_feedback_for_logger(in_logger_dict)
        if in_logger_dict['error details'] is None:
            local_logger.info(messages['success'])
        else:
            local_logger.error(messages['failed'])
            local_logger.error(in_logger_dict['error details'])

    def fn_load_file_type_csv_into_data_frame(self, local_logger, in_file_list, csv_delimiter):
        details_for_logger = self.fn_default_load_dict_message(in_file_list, 'CSV')
        out_data_frame = None
        try:
            out_data_frame = pandas.concat([pandas.read_csv(filepath_or_buffer=current_file,
                                                            delimiter=csv_delimiter,
                                                            cache_dates=True,
                                                            index_col=None,
                                                            memory_map=True,
                                                            low_memory=False,
                                                            encoding='utf-8',
                                                            ) for current_file in in_file_list])
        except Exception as err:
            details_for_logger['error details'] = err
        self.fn_file_operation_logger(local_logger, details_for_logger)
        return out_data_frame

    def fn_load_file_type_excel_into_data_frame(self, local_logger, in_file_list):
        out_data_frame = None
        details_for_logger = self.fn_default_load_dict_message(in_file_list, 'Excel')
        try:
            out_data_frame = pandas.concat([pandas.read_excel(io=current_file,
                                                              verbose=True,
                                                              ) for current_file in in_file_list])
        except Exception as err:
            details_for_logger['error details'] = err
        self.fn_file_operation_logger(local_logger, details_for_logger)
        return out_data_frame

    def fn_load_file_type_pickle_into_data_frame(self, local_logger, in_file_list,
                                                 in_compression='infer'):
        out_data_frame = None
        details_for_logger = self.fn_default_load_dict_message(in_file_list, 'Pickle')
        try:
            out_data_frame = pandas.concat([pandas.read_pickle(filepath_or_buffer=current_file,
                                                               compression=in_compression,
                                                               ) for current_file in in_file_list])
        except Exception as err:
            details_for_logger['error details'] = err
        self.fn_file_operation_logger(local_logger, details_for_logger)
        return out_data_frame

    def fn_save_data_frame_to_csv(self, local_logger, in_data_frame, in_file_details, logger_dict):
        if in_file_details['format'].lower() == 'csv':
            if 'field-delimiter' not in in_file_details:
                in_file_details['field-delimiter'] = os.pathsep
            try:
                in_data_frame.to_csv(path_or_buf=in_file_details['name'],
                                     sep=in_file_details['field-delimiter'],
                                     header=True,
                                     index=False,
                                     encoding='utf-8')
            except Exception as err:
                logger_dict['error details'] = err
            self.fn_file_operation_logger(local_logger, logger_dict)

    def fn_save_data_frame_to_excel(self, local_logger, in_data_frame, in_file_details,
                                    logger_dict):
        if in_file_details['format'].lower() == 'excel':
            try:
                in_data_frame.to_excel(excel_writer=in_file_details['name'],
                                       engine='xlsxwriter',
                                       freeze_panes=(1, 1),
                                       verbose=True)
            except Exception as err:
                logger_dict['error details'] = err
            self.fn_file_operation_logger(local_logger, logger_dict)

    def fn_save_data_frame_to_pickle(self, local_logger, in_data_frame, in_file_details,
                                     logger_dict):
        if in_file_details['format'].lower() == 'pickle':
            if 'compression' not in in_file_details:
                in_file_details['compression'] = 'gzip'
            try:
                in_data_frame.to_pickle(path=in_file_details['name'],
                                        compression=in_file_details['compression'])
            except Exception as err:
                logger_dict['error details'] = err
            self.fn_file_operation_logger(local_logger, logger_dict)

    def fn_store_data_frame_to_file(self, local_logger, timer, in_data_frame, in_file_details):
        timer.start()
        self.fn_store_data_frame_to_file_validation(local_logger, in_file_details)
        if 'format' in in_file_details:
            details_for_logger = {
                'error details': None,
                'name': in_file_details['name'],
                'format': in_file_details['format'],
                'operation': 'save',
            }
            self.fn_save_data_frame_to_csv(local_logger, in_data_frame,
                                           in_file_details, details_for_logger)
            self.fn_save_data_frame_to_excel(local_logger, in_data_frame,
                                             in_file_details, details_for_logger)
            self.fn_save_data_frame_to_pickle(local_logger, in_data_frame,
                                              in_file_details, details_for_logger)
        timer.stop()

    def fn_store_data_frame_to_file_validation(self, local_logger, in_file_details):
        if 'format' in in_file_details:
            implemented_file_formats = ['csv', 'excel', 'pickle']
            given_format = in_file_details['format'].lower()
            if given_format not in implemented_file_formats:
                local_logger.error(self.locale.gettext(
                    'File "format" attribute has a value of "{format_value}" '
                    + 'which is not among currently implemented values: '
                    + '"{implemented_file_formats}", therefore file saving is not possible')
                                   .replace('{format_value}', given_format)
                                   .replace('{implemented_file_formats}',
                                            '", "'.join(implemented_file_formats)))
        else:
            local_logger.error(self.locale.gettext('File "format" attribute is mandatory '
                                                   + 'in the file setting, but missing, '
                                                   + 'therefore file saving is not possible'))
