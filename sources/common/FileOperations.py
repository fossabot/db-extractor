"""
facilitates File Operations
"""
# package to handle date and times
from datetime import datetime
# package to add support for multi-language (i18n)
import gettext
# package to use for checksum calculations (in this file)
import hashlib
# package to handle json files
import json
# package to facilitate operating system operations
import os
# package to facilitate os path manipulations
import pathlib
# package regular expressions
import re


class FileOperations:
    timestamp_format = '%Y-%m-%d %H:%M:%S.%f %Z'
    lcl = None

    def __init__(self, default_language='en_US'):
        current_script = os.path.basename(__file__).replace('.py', '')
        lang_folder = os.path.join(os.path.dirname(__file__), 'FileOperations_Locale')
        self.lcl = gettext.translation(current_script, lang_folder, languages=[default_language])

    def build_file_list(self, local_logger, timmer, given_input_file):
        if re.search(r'(\*|\?)*', given_input_file):
            local_logger.debug(self.lcl.gettext('File matching pattern identified'))
            parent_directory = os.path.dirname(given_input_file)
            # loading from a specific folder all files matching a given pattern into a file list
            relevant_files_list = self.fn_build_relevant_file_list(local_logger, timmer,
                                                                   parent_directory,
                                                                   given_input_file)
        else:
            local_logger.debug(self.lcl.gettext('Specific file name provided'))
            relevant_files_list = [given_input_file]
        return relevant_files_list

    def fn_build_relevant_file_list(self, local_logger, timmer, in_folder, matching_pattern):
        timmer.start()
        local_logger.info(self.lcl.gettext('Listing all files within {in_folder} folder '
                                           + 'looking for {matching_pattern} as matching pattern') \
                          .replace('{in_folder}', in_folder) \
                          .replace('{matching_pattern}', matching_pattern))
        list_files = []
        file_counter = 0
        if os.path.isdir(in_folder):
            working_path = pathlib.Path(in_folder)
            for current_file in working_path.iterdir():
                if current_file.is_file() and current_file.match(matching_pattern):
                    list_files.append(file_counter)
                    list_files[file_counter] = str(current_file.absolute())
                    local_logger.info(self.lcl.gettext('{file_name} identified') \
                                      .replace('{file_name}', str(current_file.absolute())))
                    file_counter = file_counter + 1
        local_logger.info(self.lcl.ngettext( \
            '{files_counted} file from {in_folder} folder identified',
            '{files_counted} files from {in_folder} folder identified', file_counter) \
                          .replace('{files_counted}', str(file_counter)) \
                          .replace('{in_folder}', in_folder))
        timmer.stop()
        return list_files

    def fn_get_file_content(self, in_file_handler, in_file_type):
        if in_file_type == 'json':
            try:
                json_interpreted_details = json.load(in_file_handler)
                print(datetime.utcnow().strftime(self.timestamp_format) +
                      self.lcl.gettext('JSON structure interpreted'))
                return json_interpreted_details
            except Exception as e:
                print(datetime.utcnow().strftime(self.timestamp_format) +
                      self.lcl.gettext('Error encountered when trying to interpret JSON'))
                print(e)
        elif in_file_type == 'raw':
            raw_interpreted_file = in_file_handler.read()
            print(datetime.utcnow().strftime(self.timestamp_format) +
                  self.lcl.gettext('Entire file content read'))
            return raw_interpreted_file
        else:
            print(datetime.utcnow().strftime(self.timestamp_format) +
                  self.lcl.gettext('Unknown file type provided, '
                                   + 'expected either "json" or "raw" but got {in_file_type}') \
                  .replace('{in_file_type}', in_file_type))

    def fn_get_file_statistics(self, file_to_evaluate):
        try:
            file_handler = open(file=file_to_evaluate, mode='r', encoding='mbcs')
        except UnicodeDecodeError:
            file_handler = open(file=file_to_evaluate, mode='r', encoding='utf-8')
        file_content = file_handler.read().encode()
        file_handler.close()
        file_checksums = {
            'md5': hashlib.md5(file_content).hexdigest(),
            'sha1': hashlib.sha1(file_content).hexdigest(),
            'sha224': hashlib.sha224(file_content).hexdigest(),
            'sha256': hashlib.sha256(file_content).hexdigest(),
            'sha384': hashlib.sha384(file_content).hexdigest(),
            'sha512': hashlib.sha512(file_content).hexdigest(),
        }
        f_dts = {
            'created': datetime.fromtimestamp(os.path.getctime(file_to_evaluate)),
            'modified': datetime.fromtimestamp(os.path.getctime(file_to_evaluate)),
        }
        return {
            'date when created': datetime.strftime(f_dts['created'], self.timestamp_format),
            'date when last modified': datetime.strftime(f_dts['modified'], self.timestamp_format),
            'size [bytes]': os.path.getsize(file_to_evaluate),
            'MD5 Checksum': file_checksums['md5'],
            'SHA1 Checksum': file_checksums['sha1'],
            'SHA224 Checksum': file_checksums['sha224'],
            'SHA256 Checksum': file_checksums['sha256'],
            'SHA384 Checksum': file_checksums['sha384'],
            'SHA512 Checksum': file_checksums['sha512'],
        }

    def fn_move_files(self, local_logger, timmer, file_names, destination_folder):
        timmer.start()
        resulted_files = []
        for current_file in file_names:
            source_folder = os.path.dirname(current_file)
            new_file_name = current_file.replace(source_folder, destination_folder)
            if os.path.isfile(new_file_name):
                local_logger.warning(self.lcl.gettext('File {file_name} will be overwritten') \
                                     .replace('{file_name}', current_file))
                os.replace(current_file, new_file_name)
                local_logger.info(self.lcl.gettext( \
                    'File {file_name} has just been been overwritten as {new_file_name}') \
                                     .replace('{file_name}', current_file) \
                                     .replace('{new_file_name}', current_file))
            else:
                local_logger.info(self.lcl.gettext( \
                    'File {file_name} will be renamed as {new_file_name}') \
                                     .replace('{file_name}', current_file) \
                                     .replace('{new_file_name}', current_file))
                os.rename(current_file, new_file_name)
                local_logger.info(self.lcl.gettext( \
                    'File {file_name} has just been renamed as {new_file_name}') \
                                     .replace('{file_name}', current_file) \
                                     .replace('{new_file_name}', current_file))
            resulted_files.append(str(new_file_name))
        timmer.stop()
        return resulted_files

    def fn_open_file_and_get_content(self, input_file, content_type='json'):
        if os.path.isfile(input_file):
            with open(input_file, 'r') as file_handler:
                print(datetime.utcnow().strftime(self.timestamp_format) +
                      + self.lcl.gettext('File {file_name} has just been opened') \
                      .replace('{file_name}', input_file))
                return self.fn_get_file_content(file_handler, content_type)
        else:
            print(datetime.utcnow().strftime(self.timestamp_format) +
                  self.lcl.gettext('File {file_name} does not exist') \
                  .replace('{file_name}', input_file))

    def fn_store_file_statistics(self, local_logger, timmer, file_name, file_meaning):
        timmer.start()
        list_file_names = [file_name]
        if type(file_name) == list:
            list_file_names = file_name
        for current_file_name in list_file_names:
            file_statistics = str(self.fn_get_file_statistics(current_file_name)) \
                .replace('date when created', self.lcl.gettext('date when created')) \
                .replace('date when last modified', self.lcl.gettext('date when last modified')) \
                .replace('size [bytes]', self.lcl.gettext('size [bytes]')) \
                .replace('Checksum', self.lcl.gettext('Checksum'))
            local_logger.info(self.lcl.gettext( \
                'File "{file_name}" has the following characteristics: {file_statistics}') \
                              .replace('{file_meaning}', file_meaning) \
                              .replace('{file_name}', current_file_name) \
                              .replace('{file_statistics}', file_statistics))
        timmer.stop()
