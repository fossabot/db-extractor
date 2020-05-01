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

    @staticmethod
    def fn_save_data_frame_to_csv(in_data_frame, in_file_details):
        if 'field-delimiter' not in in_file_details:
            in_file_details['field-delimiter'] = os.pathsep
        in_data_frame.to_csv(path_or_buf=in_file_details['name'],
                             sep=in_file_details['field-delimiter'],
                             header=True,
                             index=False,
                             encoding='utf-8')

    @staticmethod
    def fn_save_data_frame_to_pickle(in_data_frame, in_file_details):
        if 'compression' not in in_file_details:
            in_file_details['compression'] = 'gzip'
        in_data_frame.to_pickle(path=in_file_details['name'],
                                compression=in_file_details['compression'])

    def fn_store_data_frame_to_file(self, local_logger, timmer, in_data_frame, in_file_details):
        timmer.start()
        is_file_saved = False
        given_format = self.fn_store_data_frame_to_file_validation(local_logger, in_file_details)
        if given_format == 'csv':
            self.fn_save_data_frame_to_csv(in_data_frame, in_file_details)
            is_file_saved = True
        elif given_format == 'excel':
            in_data_frame.to_excel(excel_writer=in_file_details['name'],
                                   engine='xlsxwriter',
                                   freeze_panes=(1, 1),
                                   verbose=True)
            is_file_saved = True
        elif given_format == 'pickle':
            self.fn_save_data_frame_to_pickle(in_data_frame, in_file_details)
            is_file_saved = True
        if is_file_saved:
            local_logger.info(self.lcl.gettext(
                'Pandas Data Frame has just been saved to file "{file_name}", '
                + 'considering {file_type} as file type')
                              .replace('{file_name}', in_file_details['name'])
                              .replace('{file_type}', in_file_details['format']))
        timmer.stop()

    def fn_store_data_frame_to_file_validation(self, local_logger, in_file_details):
        are_settings_ok = False
        if 'format' in in_file_details:
            implemented_file_formats = ['csv', 'excel', 'pickle']
            given_format = in_file_details['format'].lower()
            if given_format in implemented_file_formats:
                are_settings_ok = given_format
            else:
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
        return are_settings_ok
