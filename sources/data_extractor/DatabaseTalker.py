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
    def execute_query(local_logger, timered, given_cursor, given_query):
        try:
            timered.start()
            given_cursor.execute(given_query)
            pt = timedelta(microseconds=(given_cursor.server_processing_time() / 1000))
            local_logger.info('Query executed successfully ' + format(pt))
            timered.stop()
            return given_cursor
        except TypeError as e:
            local_logger.error('Error running the query: ')
            local_logger.error(e)
            timered.stop()

    @staticmethod
    def fetch_executed_query(local_logger, timered, given_cursor):
        timered.start()
        local_result_set = given_cursor.fetchall()
        local_logger.info('Result-set has been completely fetched and contains '
                          + str(len(local_result_set)) + ' rows')
        timered.stop()
        return local_result_set

    @staticmethod
    def result_set_to_data_frame(local_logger, timered, given_result_set, given_columns_name):
        timered.start()
        df = pd.DataFrame(given_result_set, index=None, columns=given_columns_name)
        local_logger.info('Result-set has been loaded into Pandas DataFrame')
        timered.stop()
        return df
