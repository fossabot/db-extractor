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
# package to bring ability to check hostname availability
import socket


class DatabaseTalker:
    connection = None
    locale = None

    def __init__(self, in_language='en_US'):
        file_parts = os.path.normpath(os.path.abspath(__file__)).replace('\\', os.path.altsep)\
            .split(os.path.altsep)
        locale_domain = file_parts[(len(file_parts)-1)].replace('.py', '')
        locale_folder = os.path.normpath(os.path.join(
            os.path.join(os.path.altsep.join(file_parts[:-2]), 'project_locale'), locale_domain))
        self.locale = gettext.translation(locale_domain, localedir=locale_folder,
                                          languages=[in_language], fallback=True)

    def append_additional_columns_to_df(self, local_logger, timer, data_frame, session_details):
        resulted_data_frame = data_frame
        timer.start()
        for crt_column in session_details['additional-columns']:
            if crt_column['value'] == 'utcnow':
                resulted_data_frame[crt_column['name']] = datetime.utcnow()
            elif crt_column['value'] == 'now':
                resulted_data_frame[crt_column['name']] = datetime.now()
            else:
                resulted_data_frame[crt_column['name']] = crt_column['value']
        local_logger.info(self.locale.ngettext(
            'Additional {additional_columns_counted} column added to Pandas Data Frame',
            'Additional {additional_columns_counted} columns added to Pandas Data Frame',
                          len(session_details['additional-columns']))
                          .replace('{additional_columns_counted}',
                                   str(len(session_details['additional-columns']))))
        timer.stop()
        return resulted_data_frame

    def connect_to_database(self, local_logger, timer, connection_details):
        timer.start()
        local_logger.info(self.locale.gettext(
            'Connection to {server_vendor_and_type} server, layer {server_layer} '
            + 'which means (server {server_name}, port {server_port}) '
            + 'using the username {username} ({name_of_user})')
                          .replace('{server_vendor_and_type}',
                                   connection_details['server-vendor-and-type'])
                          .replace('{server_layer}', connection_details['server-layer'])
                          .replace('{server_name}', connection_details['ServerName'])
                          .replace('{server_port}', str(connection_details['ServerPort']))
                          .replace('{username}', connection_details['Username'])
                          .replace('{name_of_user}', connection_details['Name']))
        try:
            socket.gethostbyname(connection_details['ServerName'])
            if connection_details['server-vendor-and-type'] == 'SAP HANA':
                self.connect_to_database_hana(local_logger, connection_details)
            elif connection_details['server-vendor-and-type'] in ('MariaDB Foundation MariaDB',
                                                                  'Oracle MySQL'):
                self.connect_to_database_mysql(local_logger, connection_details)
        except socket.gaierror as err:
            local_logger.error('Hostname not found, connection will not be established')
            local_logger.error(err)
        timer.stop()

    def connect_to_database_hana(self, local_logger, connection_details):
        try:
            self.connection = dbapi.connect(
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
            local_logger.info(self.locale.gettext(
                'Connection to {server_vendor_and_type} server completed')
                              .replace('{server_vendor_and_type}',
                                       connection_details['server-vendor-and-type']))
        except ConnectionError as err:
            local_logger.error(self.locale.gettext(
                'Error connecting to {server_vendor_and_type} server with details')
                              .replace('{server_vendor_and_type}',
                                       connection_details['server-vendor-and-type']))
            local_logger.error(err)

    def connect_to_database_mysql(self, local_logger, connection_details):
        try:
            self.connection = mysql.connector.connect(
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
            local_logger.info(self.locale.gettext(
                'Connection to {server_vendor_and_type} server completed')
                              .replace('{server_vendor_and_type}',
                                       connection_details['server-vendor-and-type']))
        except mysql.connector.Error as err:
            local_logger.error(self.locale.gettext(
                'Error connecting to {server_vendor_and_type} server with details')
                              .replace('{server_vendor_and_type}',
                                       connection_details['server-vendor-and-type']))
            local_logger.error(err)

    def execute_query(self, local_logger, timer, in_cursor, in_query, in_counted_parameters,
                      in_tuple_parameters):
        try:
            timer.start()
            if in_counted_parameters > 0:
                in_cursor.execute(in_query % in_tuple_parameters)
            else:
                in_cursor.execute(in_query)
            try:
                processing_tm = timedelta(microseconds=(in_cursor.server_processing_time() / 1000))
                local_logger.info(self.locale.gettext(
                    'Query executed successfully '
                    + 'having a server processing time of {processing_time}')
                                  .replace('{processing_time}', format(processing_tm)))
            except AttributeError:
                local_logger.info(self.locale.gettext('Query executed successfully'))
            timer.stop()
            return in_cursor
        except dbapi.ProgrammingError as e:
            local_logger.error(self.locale.gettext('Error running the query:'))
            local_logger.error(e)
            timer.stop()

    def fetch_executed_query(self, local_logger, timer, given_cursor):
        timer.start()
        local_result_set = None
        try:
            local_result_set = given_cursor.fetchall()
            local_logger.info(self.locale.gettext(
                'Result-set has been completely fetched and contains {rows_counted} rows')
                              .replace('{rows_counted}', str(len(local_result_set))))
        except ConnectionError as e:
            local_logger.info(self.locale.gettext('Connection problem encountered: '))
            local_logger.info(e)
        timer.stop()
        return local_result_set

    def get_column_names(self, local_logger, timer, given_cursor):
        timer.start()
        try:
            column_names = given_cursor.column_names
        except AttributeError:
            column_names = []
            for column_name, col2, col3, col4, col5, col6, col7 in given_cursor.description:
                column_names.append(column_name)
        local_logger.info(self.locale.gettext(
            'Result-set column name determination completed: {columns_name}')
                          .replace('{columns_name}', str(column_names)))
        timer.stop()
        return column_names

    def result_set_to_data_frame(self, local_logger, timer, given_columns_name, given_result_set):
        timer.start()
        df = pd.DataFrame(data=given_result_set, index=None, columns=given_columns_name)
        local_logger.info(self.locale.gettext('Result-set has been loaded into Pandas Data Frame'))
        timer.stop()
        return df
