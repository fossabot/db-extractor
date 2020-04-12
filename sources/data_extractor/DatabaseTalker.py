"""
CommandLineArgumentManagement - library to manage input parameters from command line

This library allows handling pre-configured arguments to be received from command line and use them
to call the main package functions
"""
# package helping out to work with SAP HANA
from hdbcli import dbapi
# package to facilitate time operations
from datetime import datetime, timedelta
# package facilitating Data Frames manipulation
import pandas as pd


class DatabaseTalker:
    conn = None

    @staticmethod
    def append_additional_columns_to_data_frame(local_logger, timered, data_frame, session_details):
        if 'additional-columns' in session_details:
            timered.start()
            for crt_column in session_details['additional-columns']:
                if crt_column['value'] == 'utcnow': # special case
                    data_frame[crt_column['name']] = datetime.utcnow()
                else:
                    data_frame[crt_column['name']] = crt_column['value']
            local_logger.info('Additional column(s) have been added to Pandas DataFrame')
            timered.stop()

    def connect_to_sap_hana(self, local_logger, timered, connection_details):
        timered.start()
        local_logger.info('I will attempt to connect to SAP HANA server, layer '
                          + connection_details['server-layer'] + ' which means ('
                          + 'server ' + connection_details['ServerName']
                          + ', port ' + str(connection_details['ServerPort'])
                          + ') using the username ' + connection_details['Username']
                          + ' (' + connection_details['Name'] + ')')
        try:
            # create actual connection
            self.conn = dbapi.connect(
                address=connection_details['ServerName'],
                port=connection_details['ServerPort'],
                user=connection_details['Username'],
                password=connection_details['Password'],
            )
            local_logger.info('Connecting to SAP HANA server completed')
            timered.stop()
        except Exception as e:
            local_logger.error('Error in Connection with details: ')
            local_logger.error(e)
            timered.stop()

    @staticmethod
    def determine_column_names(local_logger, timered, cursor_frame):
        timered.start()
        column_names = []
        for column_name, col2, col3, col4, col5, col6, col7 in cursor_frame.description:
            column_names.append(column_name)
        local_logger.info('Result-set column name determination completed')
        timered.stop()
        return column_names

    @staticmethod
    def execute_query(self, local_logger, timered, query_session_details):
        try:
            timered.start()
            query_session_details['cursor'].execute(query_session_details['query'])
            pt = timedelta(microseconds=(query_session_details['cursor'].server_processing_time()
                                         / 1000))
            local_logger.info('Query executed successfully ' + format(pt))
            timered.stop()
        except TypeError as e:
            local_logger.error('Error running the query: ')
            local_logger.error(e)
            timered.stop()

    @staticmethod
    def fetch_executed_query(local_logger, timered, query_session_details):
        timered.start()
        result_set = query_session_details['cursor'].fetchall()
        local_logger.info('Result-set has been completely fetched and contains '
                          + str(len(result_set)) + ' rows')
        timered.stop()
        return result_set

    @staticmethod
    def resultset_to_dataframe(local_logger, timered, packed_result_set):
        timered.start()
        df = pd.DataFrame(packed_result_set['result-set'],
                          index=None,
                          columns=packed_result_set['csv-columns'])
        local_logger.info('Result-set has been loaded into Pandas DataFrame')
        timered.stop()
