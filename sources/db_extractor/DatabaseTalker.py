"""
DatabaseTalker - library to facilitate database communication
"""
# package to facilitate time operations
from datetime import datetime, timedelta
# package to add support for multi-language (i18n)
import gettext
# package helping out to work with SAP HANA
from hdbcli import dbapi
# package helping out to work with Oracle MySQL
import mysql.connector
import mysql.connector.errors
# package to handle files/folders and related metadata/operations
import os
# package facilitating Data Frames manipulation
import pandas as pd


class DatabaseTalker:
    conn = None
    lcl = None

    def __init__(self, default_language='en_US'):
        current_script = os.path.basename(__file__).replace('.py', '')
        lang_folder = os.path.join(os.path.dirname(__file__), current_script + '_Locale')
        self.lcl = gettext.translation(current_script, lang_folder, languages=[default_language])

    def append_additional_columns_to_df(self, local_logger, timered, data_frame, session_details):
        resulted_data_frame = data_frame
        if 'additional-columns' in session_details:
            timered.start()
            for crt_column in session_details['additional-columns']:
                if crt_column['value'] == 'utcnow':
                    resulted_data_frame[crt_column['name']] = datetime.utcnow()
                elif crt_column['value'] == 'now':
                    resulted_data_frame[crt_column['name']] = datetime.now()
                else:
                    resulted_data_frame[crt_column['name']] = crt_column['value']
            local_logger.info(self.lcl.ngettext( \
                'Additional {additional_columns_counted} column added to Pandas Data Frame',
                'Additional {additional_columns_counted} columns added to Pandas Data Frame') \
                              .replace('{additional_columns_counted}',
                                       str(len(session_details['additional-columns']))))
            timered.stop()
        return resulted_data_frame

    def connect_to_database(self, local_logger, timered, connection_details):
        timered.start()
        local_logger.info(self.lcl.gettext( \
            'Connection to {server_vendor_and_type} server, layer {server_layer} '
            + 'which means (server {server_name}, port {server_port}) '
            + 'using the username {username} ({name_of_user})') \
                          .replace('{server_vendor_and_type}',
                                   connection_details['server-vendor-and-type']) \
                          .replace('{server_layer}', connection_details['server-layer']) \
                          .replace('{server_name}', connection_details['ServerName']) \
                          .replace('{server_port}', connection_details['ServerPort']) \
                          .replace('{username}', connection_details['Username']) \
                          .replace('{name_of_user}', connection_details['Name']))
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
            local_logger.info(self.lcl.gettext( \
                'Connection to {server_vendor_and_type} server completed') \
                              .replace('{server_vendor_and_type}',
                                       connection_details['server-vendor-and-type']))
        except ConnectionError as err:
            local_logger.error(self.lcl.gettext( \
                'Error connecting to {server_vendor_and_type} server with details') \
                              .replace('{server_vendor_and_type}',
                                       connection_details['server-vendor-and-type']))
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
            local_logger.info(self.lcl.gettext( \
                'Connection to {server_vendor_and_type} server completed') \
                              .replace('{server_vendor_and_type}',
                                       connection_details['server-vendor-and-type']))
        except mysql.connector.Error as err:
            local_logger.error(self.lcl.gettext( \
                'Error connecting to {server_vendor_and_type} server with details') \
                              .replace('{server_vendor_and_type}',
                                       connection_details['server-vendor-and-type']))
            local_logger.error(err)

    def execute_query(self, local_logger, timered, in_cursor, in_query, in_counted_parameters,
                      in_tuple_parameters):
        try:
            timered.start()
            if in_counted_parameters > 0:
                in_cursor.execute(in_query % in_tuple_parameters)
            else:
                in_cursor.execute(in_query)
            try:
                processing_tm = timedelta(microseconds=(in_cursor.server_processing_time() / 1000))
                local_logger.info(self.lcl.gettext( \
                    'Query executed successfully '
                    + 'having a server processing time of {processing_time}') \
                                  .replace('{processing_time}', format(processing_tm)))
            except AttributeError:
                local_logger.info(self.lcl.gettext('Query executed successfully'))
            timered.stop()
            return in_cursor
        except TypeError as e:
            local_logger.error(self.lcl.gettext('Error running the query:'))
            local_logger.error(e)
            timered.stop()

    def fetch_executed_query(self, local_logger, timered, given_cursor):
        timered.start()
        local_result_set = None
        try:
            local_result_set = given_cursor.fetchall()
            local_logger.info(self.lcl.gettext( \
                'Result-set has been completely fetched and contains {rows_counted} rows') \
                              .replace('{rows_counted}', str(len(local_result_set))))
        except ConnectionError as e:
            local_logger.info(self.lcl.gettext('Connection problem encountered: '))
            local_logger.info(e)
        timered.stop()
        return local_result_set

    def get_column_names(self, local_logger, timered, given_cursor):
        timered.start()
        try:
            column_names = given_cursor.column_names
        except AttributeError:
            column_names = []
            for column_name, col2, col3, col4, col5, col6, col7 in given_cursor.description:
                column_names.append(column_name)
        local_logger.info(self.lcl.gettext( \
            'Result-set column name determination completed: {columns_name}') \
                          .replace('{columns_name}', str(column_names)))
        timered.stop()
        return column_names

    def result_set_to_data_frame(self, local_logger, timered, given_columns_name, given_result_set):
        timered.start()
        df = pd.DataFrame(data=given_result_set, index=None, columns=given_columns_name)
        local_logger.info(self.lcl.gettext('Result-set has been loaded into Pandas Data Frame'))
        timered.stop()
        return df
