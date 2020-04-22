"""
facilitates File Operations
"""
# package to handle date and times
from datetime import datetime
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

    def build_file_list(self, local_logger, timmer, given_input_file):
        if re.search(r'(\*|\?)*', given_input_file):
            local_logger.debug('File pattern identified')
            parent_directory = os.path.dirname(given_input_file)
            # loading from a specific folder all files matching a given pattern into a file list
            relevant_files_list = self.fn_build_relevant_file_list(local_logger, timmer,
                                                                   parent_directory,
                                                                   given_input_file)
        else:
            local_logger.debug('Specific file name provided')
            relevant_files_list = [given_input_file]
        return relevant_files_list

    @staticmethod
    def fn_build_relevant_file_list(local_logger, timmer, in_folder, matching_pattern):
        timmer.start()
        local_logger.info('Listing all files within ' + in_folder
                          + ' folder looking for ' + matching_pattern + ' as matching pattern')
        list_files = []
        file_counter = 0
        if os.path.isdir(in_folder):
            working_path = pathlib.Path(in_folder)
            for current_file in working_path.iterdir():
                if current_file.is_file() and current_file.match(matching_pattern):
                    list_files.append(file_counter)
                    list_files[file_counter] = str(current_file.absolute())
                    local_logger.info(str(current_file.absolute()) + ' file identified')
                    file_counter = file_counter + 1
        local_logger.info(str(file_counter) + ' file(s) from ' + in_folder + ' folder identified!')
        timmer.stop()
        return list_files

    def fn_get_file_content(self, in_file_handler, in_content_type):
        if in_content_type == 'json':
            try:
                json_interpreted_details = json.load(in_file_handler)
                print(datetime.utcnow().strftime(self.timestamp_format) +
                      'I have interpreted JSON structure from given file')
                return json_interpreted_details
            except Exception as e:
                print(datetime.utcnow().strftime(self.timestamp_format) +
                      'Error encountered when trying to interpret JSON')
                print(e)
        elif in_content_type == 'raw':
            raw_interpreted_file = in_file_handler.read()
            print(datetime.utcnow().strftime(self.timestamp_format) +
                  'I have read file entire content')
            return raw_interpreted_file
        else:
            print(datetime.utcnow().strftime(self.timestamp_format) +
                  'Unknown content type provided, expected either "json" or "raw" but got '
                  + in_content_type)

    @staticmethod
    def fn_get_file_statistics(file_to_evaluate):
        try:
            file_handler = open(file=file_to_evaluate, mode='r', encoding='mbcs')
        except UnicodeDecodeError:
            file_handler = open(file=file_to_evaluate, mode='r', encoding='utf-8')
        file_content = file_handler.read().encode()
        file_handler.close()
        file_checksums = {
            'md5': hashlib.md5(file_content).hexdigest(),
            'sha1': hashlib.sha1(file_content).hexdigest(),
            'sha256': hashlib.sha256(file_content).hexdigest(),
            'sha512': hashlib.sha512(file_content).hexdigest(),
        }
        f_dts = {
            'created': datetime.fromtimestamp(os.path.getctime(file_to_evaluate)),
            'modified': datetime.fromtimestamp(os.path.getctime(file_to_evaluate)),
        }
        return {
            'date when created': datetime.strftime(f_dts['created'], '%Y-%m-%d %H:%M:%S.%f'),
            'date when last modified': datetime.strftime(f_dts['modified'], '%Y-%m-%d %H:%M:%S.%f'),
            'size [bytes]': os.path.getsize(file_to_evaluate),
            'MD5-Checksum': file_checksums['md5'],
            'SHA256-Checksum': file_checksums['sha256'],
            'SHA512-Checksum': file_checksums['sha512'],
        }

    @staticmethod
    def fn_move_files(local_logger, timmer, source_folder, file_names, destination_folder):
        timmer.start()
        resulted_files = []
        for current_file in file_names:
            new_file_name = current_file.replace(source_folder, destination_folder)
            if new_file_name.is_file():
                os.replace(current_file, new_file_name)
                local_logger.info('File ' + current_file
                                  + ' has just been been overwritten  as ' + new_file_name)
            else:
                os.rename(current_file, new_file_name)
                local_logger.info('File ' + current_file
                                  + ' has just been renamed as ' + new_file_name)
            resulted_files.append(new_file_name)
        timmer.stop()
        return resulted_files

    def fn_open_file_and_get_content(self, input_file, content_type='json'):
        if os.path.isfile(input_file):
            with open(input_file, 'r') as file_handler:
                print(datetime.utcnow().strftime(self.timestamp_format) +
                      'I have opened file: ' + input_file)
                return self.fn_get_file_content(file_handler, content_type)
        else:
            print(datetime.utcnow().strftime(self.timestamp_format) +
                  'Given file ' + input_file + ' does not exist, please check your inputs!')

    def fn_store_file_statistics(self, local_logger, timmer, file_name, file_meaning):
        timmer.start()
        file_name_variable_type = str(type(file_name))
        list_file_names = [file_name]
        if file_name_variable_type == "<class 'list'>":
            list_file_names = file_name
        for current_file_name in list_file_names:
            local_logger.info(file_meaning + ' file "' + current_file_name
                              + '" has the following characteristics: '
                              + str(self.fn_get_file_statistics(current_file_name)))
        timmer.stop()
