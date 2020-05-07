# useful methods to measure time performance by small pieces of code
from codetiming import Timer
# Custom package specific to this project
from db_extractor.BasicNeeds import BasicNeeds
from db_extractor.LoggingNeeds import LoggingNeeds
from db_extractor.FileOperations import datetime, os, FileOperations
from db_extractor.ParameterHandling import ParameterHandling
# get current script name
CURRENT_SCRIPT_NAME = os.path.basename(__file__).replace('.py', '')
SCRIPT_LANGUAGE = 'ro_RO'
REFERENCE_EXPRESION = 'CalculatedDate_CYCMCDCH_-48'

# instantiate Logger class
c_ln = LoggingNeeds()
# initiate logger
c_ln.initiate_logger(CURRENT_SCRIPT_NAME + '.log', CURRENT_SCRIPT_NAME)
# instantiate File Operations class
c_fo = FileOperations(SCRIPT_LANGUAGE)
# instantiate File Operations class
c_ph = ParameterHandling(SCRIPT_LANGUAGE)
older_to_newer_barrier = c_ph.eval_expression(c_ln.logger, REFERENCE_EXPRESION, 1)
barrier = datetime.strptime(older_to_newer_barrier, c_ph.output_standard_formats.get('hour'))
print('Older to Newer barrier is ' + str(barrier))
# define global timer to use
t = Timer(CURRENT_SCRIPT_NAME, text = 'Time spent is {seconds}', logger = c_ln.logger.debug)
# pick all JSON files from this folder
relevant_files = c_fo.fn_build_relevant_file_list(c_ln.logger, t,
                                                  os.path.dirname(__file__), '*.json')
for current_file in relevant_files:
    current_file_modified_datetime = datetime.fromtimestamp(os.path.getmtime(current_file))
    file_verdict = c_fo.fn_get_file_datetime_verdict(c_ln.logger, current_file,
                                                     'last modified', barrier)
    print(current_file + ' => ' + str(current_file_modified_datetime)
          + ' vs. ' + str(barrier) + ' => ' + file_verdict)
# instantiate Basic Needs class
c_bn = BasicNeeds(SCRIPT_LANGUAGE)
# just final message
c_bn.fn_final_message(c_ln.logger, CURRENT_SCRIPT_NAME + '.log',
                      t.timers.total(CURRENT_SCRIPT_NAME))
