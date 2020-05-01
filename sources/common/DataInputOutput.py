"""
Data Input Output class
"""
# package to add support for multi-language (i18n)
import gettext
# package to handle files/folders and related metadata/operations
import os
# package facilitating Data Frames manipulation
import pandas as pd


class DataInputOutput:
    lcl = None

    def __init__(self, default_language='en_US'):
        current_script = os.path.basename(__file__).replace('.py', '')
        lang_folder = os.path.join(os.path.dirname(__file__), current_script + '_Locale')
        self.lcl = gettext.translation(current_script, lang_folder, languages=[default_language])

    def fn_file_save_error_logger(self, local_logger, in_file_details, error_details):
        if error_details is None:
            local_logger.info(self.lcl.gettext(
                'Pandas Data Frame has just been saved to file "{file_name}", '
                + 'considering {file_type} as file type')
                              .replace('{file_name}', in_file_details['name'])
                              .replace('{file_type}', in_file_details['format']))
        else:
            local_logger.error(
                self.lcl.gettext(
                    'Error encountered on saving Pandas Data Frame '
                    + 'into a {file_type} file type (see below)')
                    .replace('{file_type}', in_file_details['format'].upper()))
            local_logger.error(error_details)

    def fn_load_file_list_to_data_frame(self, local_logger, timmer, file_list, csv_delimiter):
        timmer.start()
        out_data_frame = pd.concat([pd.read_csv(filepath_or_buffer=current_file,
                                                delimiter=csv_delimiter,
                                                cache_dates=True,
                                                index_col=None,
                                                memory_map=True,
                                                low_memory=False,
                                                encoding='utf-8',
                                                ) for current_file in file_list])
        local_logger.info(self.lcl.gettext(
            'All relevant files ({files_counted}) were merged into a Pandas Data Frame')
                          .replace('{files_counted}', str(len(file_list))))
        timmer.stop()
        return out_data_frame

    def fn_save_data_frame_to_csv(self, local_logger, in_data_frame, in_file_details):
        if in_file_details['format'].lower() == 'csv':
            if 'field-delimiter' not in in_file_details:
                in_file_details['field-delimiter'] = os.pathsep
            try:
                in_data_frame.to_csv(path_or_buf=in_file_details['name'],
                                     sep=in_file_details['field-delimiter'],
                                     header=True,
                                     index=False,
                                     encoding='utf-8')
                self.fn_file_save_error_logger(local_logger, in_file_details, None)
            except Exception as err:
                self.fn_file_save_error_logger(local_logger, in_file_details, err)

    def fn_save_data_frame_to_excel(self, local_logger, in_data_frame, in_file_details):
        if in_file_details['format'].lower() == 'excel':
            try:
                in_data_frame.to_excel(excel_writer=in_file_details['name'],
                                       engine='xlsxwriter',
                                       freeze_panes=(1, 1),
                                       verbose=True)
                self.fn_file_save_error_logger(local_logger, in_file_details, None)
            except Exception as err:
                self.fn_file_save_error_logger(local_logger, in_file_details, err)

    def fn_save_data_frame_to_pickle(self, local_logger, in_data_frame, in_file_details):
        if in_file_details['format'].lower() == 'pickle':
            if 'compression' not in in_file_details:
                in_file_details['compression'] = 'gzip'
            try:
                in_data_frame.to_pickle(path=in_file_details['name'],
                                        compression=in_file_details['compression'])
                self.fn_file_save_error_logger(local_logger, in_file_details, None)
            except Exception as err:
                self.fn_file_save_error_logger(local_logger, in_file_details, err)

    def fn_store_data_frame_to_file(self, local_logger, timmer, in_data_frame, in_file_details):
        timmer.start()
        self.fn_store_data_frame_to_file_validation(local_logger, in_file_details)
        if 'format' in in_file_details:
            self.fn_save_data_frame_to_csv(local_logger, in_data_frame, in_file_details)
            self.fn_save_data_frame_to_excel(local_logger, in_data_frame, in_file_details)
            self.fn_save_data_frame_to_pickle(local_logger, in_data_frame, in_file_details)
        timmer.stop()

    def fn_store_data_frame_to_file_validation(self, local_logger, in_file_details):
        if 'format' in in_file_details:
            implemented_file_formats = ['csv', 'excel', 'pickle']
            given_format = in_file_details['format'].lower()
            if given_format not in implemented_file_formats:
                local_logger.error(self.lcl.gettext(
                    'File "format" attribute has a value of "{format_value}" '
                    + 'which is not among currently implemented values: '
                    + '"{implemented_file_formats}", therefore file saving is not possible')
                                   .replace('{format_value}', given_format)
                                   .replace('{implemented_file_formats}',
                                            '", "'.join(implemented_file_formats)))
        else:
            local_logger.error(self.lcl.gettext('File "format" attribute is mandatory '
                                                + 'in the file setting, but missing, '
                                                + 'therefore file saving is not possible'))
