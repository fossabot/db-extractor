"""
CommandLineArgumentManagement - library to manage input parameters from command line

This library allows handling pre-configured arguments to be received from command line and use them
to call the main package functions
"""
# package helping out to work with SAP HANA
from hdbcli import dbapi
# package helping out to work with Oracle MySQL
import mysql.connector
from mysql.connector import errorcode
# package to facilitate time operations
from datetime import datetime, timedelta
# package facilitating Data Frames manipulation
import pandas as pd


class DatabaseTalker:
    conn = None

    @staticmethod
    def append_additional_columns_to_df(local_logger, timered, data_frame, session_details):
        resulted_data_frame = data_frame
        if 'additional-columns' in session_details:
            timered.start()
            for crt_column in session_details['additional-columns']:
                if crt_column['value'] == 'utcnow': # special case
                    resulted_data_frame[crt_column['name']] = datetime.utcnow()
                else:
                    resulted_data_frame[crt_column['name']] = crt_column['value']
            local_logger.info('Additional column(s) have been added to Pandas DataFrame')
            timered.stop()
        return resulted_data_frame

    def connect_to_database(self, local_logger, timered, connection_details):
        timered.start()
        local_logger.info('I will attempt to connect to '
                          + connection_details['server-vendor-and-type'] + ' server, layer '
                          + connection_details['server-layer'] + ' which means ('
                          + 'server ' + connection_details['ServerName']
                          + ', port ' + str(connection_details['ServerPort'])
                          + ') using the username ' + connection_details['Username']
                          + ' (' + connection_details['Name'] + ')')
        if connection_details['server-vendor-and-type'] == 'SAP HANA':
            self.connect_to_database_hana(local_logger, connection_details)
        elif connection_details['server-vendor-and-type'] in ('MariaDB Foundation MariaDB',
                                                              'Oracle MySQL'):
            self.connect_to_database_mysql(local_logger, connection_details)
        timered.stop()

    def connect_to_database_hana(self, local_logger, connection_details):
        try:
            self.conn = dbapi.connect(
                address=connection_details['ServerName'],
                port=connection_details['ServerPort'],
                user=connection_details['Username'],
                password=connection_details['Password'],
                prefetch='FALSE',
                chopBlanks='TRUE',
                compress='TRUE',
                connDownRollbackError='TRUE',
                statementCacheSize=10,
            )
            local_logger.info('Connecting to  ' + connection_details['server-vendor-and-type']
                              + ' server completed')
        except mysql.connector.Error as err:
            local_logger.error('Error connecting to MySQL with details: ')
            local_logger.error(err)

    def connect_to_database_mysql(self, local_logger, connection_details):
        try:
            self.conn = mysql.connector.connect(
                host=connection_details['ServerName'],
                port=connection_details['ServerPort'],
                user=connection_details['Username'],
                password=connection_details['Password'],
                database='mysql',
                compress=True,
                autocommit=True,
                use_unicode=True,
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                get_warnings=True,
            )
            local_logger.info('Connecting to  ' + connection_details['server-vendor-and-type']
                              + ' server completed')
        except mysql.connector.Error as err:
            local_logger.error('Error connecting to MySQL with details: ')
            local_logger.error(err)

    @staticmethod
    def execute_query(local_logger, timered, in_cursor, in_query, in_counted_parameters,
                      in_tuple_parameters):
        try:
            timered.start()
            if in_counted_parameters > 0:
                in_cursor.execute(in_query % in_tuple_parameters)
            else:
                in_cursor.execute(in_query)
            try:
                processing_tm = timedelta(microseconds=(in_cursor.server_processing_time() / 1000))
                local_logger.info('Query executed successfully ' + format(processing_tm))
            except AttributeError:
                local_logger.info('Query executed successfully')
            timered.stop()
            return in_cursor
        except TypeError as e:
            local_logger.error('Error running the query: ')
            local_logger.error(e)
            timered.stop()

    @staticmethod
    def fetch_executed_query(local_logger, timered, given_cursor):
        timered.start()
        local_result_set = None
        try:
            local_result_set = given_cursor.fetchall()
            local_logger.info('Result-set has been completely fetched and contains '
                              + str(len(local_result_set)) + ' rows')
        except ConnectionError as e:
            local_logger.info('There was a problem with connection')
            local_logger.info(e)
        timered.stop()
        return local_result_set

    @staticmethod
    def get_column_names(local_logger, timered, given_cursor):
        timered.start()
        try:
            column_names = given_cursor.column_names
        except AttributeError:
            column_names = []
            for column_name, col2, col3, col4, col5, col6, col7 in given_cursor.description:
                column_names.append(column_name)
        local_logger.info('Result-set column name determination completed')
        timered.stop()
        return column_names

    @staticmethod
    def result_set_to_data_frame(local_logger, timered, given_columns_name, given_result_set):
        timered.start()
        df = pd.DataFrame(data=given_result_set, index=None, columns=given_columns_name)
        local_logger.info('Result-set has been loaded into Pandas DataFrame')
        timered.stop()
        return df
