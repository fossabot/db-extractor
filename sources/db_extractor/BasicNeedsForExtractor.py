"""
Class Extractor Specific Needs

Handling specific needs for Extractor script
"""
# package to facilitate working with directories and files
from pathlib import Path
# package to facilitate common operations
from db_extractor.BasicNeeds import BasicNeeds


class BasicNeedsForExtractor:
    lcl_bn = None

    def __init__(self):
        self.lcl_bn = BasicNeeds()

    def fn_check_inputs_specific(self, input_parameters):
        self.lcl_bn.fn_validate_single_value(input_parameters.input_source_system_file,
                                             'file', 'source system file')
        self.lcl_bn.fn_validate_single_value(input_parameters.input_credentials_file,
                                             'file', 'credentials file')
        self.lcl_bn.fn_validate_single_value(input_parameters.input_extracting_sequence_file,
                                             'file', 'extracting sequence file')

    @staticmethod
    def fn_is_extraction_neccesary(local_logger, relevant_details):
        extraction_is_necessary = False
        local_logger.debug('Extract behaviour is set to ' + relevant_details['extract-behaviour'])
        if relevant_details['extract-behaviour'] == \
                'skip-if-output-file-exists':
            if Path(relevant_details['output-csv-file']).is_file():
                local_logger.debug('File ' + relevant_details['output-csv-file']
                                   + ' already exists, '
                                   + 'so database extraction will not be performed')
            else:
                extraction_is_necessary = True
                local_logger.debug('File ' + relevant_details['output-csv-file']
                                   + ' does not exist, '
                                   + 'so database extraction has to be performed')
        elif relevant_details['extract-behaviour'] == \
                'overwrite-if-output-file-exists':
            extraction_is_necessary = True
            local_logger.debug('Database extraction has to be performed')
        return extraction_is_necessary
