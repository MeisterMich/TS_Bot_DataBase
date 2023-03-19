import logging
import psycopg2


logging.basicConfig(level='DEBUG')
logger = logging.getLogger()


def db_connect(database_name, self_name, self_password, self_host, self_port):
    '''This function is connected to
    database that has needed parameters and
    return connection.
    If your database wouldn't be found,
    you will recieve relevant message
    in terminal with exception,
    '''
    try:
        connection = psycopg2.connect(dbname=database_name, user=self_name, password=self_password, host=self_host, port=self_port)
        logger.info('Connected successfully!')
        return connection
    except Exception as error:
        logger.critical(f'Can not connected to data base error: {error}')
        return


translator = {'None': 'NULL',
              'none': 'NULL',
              'NONE': 'NULL',
              'bool': 'BOOL',
              'Bool': 'BOOL',
              'int': 'INTEGER',
              'Int': 'INTEGER',
              'INT': 'LNTEGER',
              'long': 'BIGINT',
              'Long': 'BIGINT',
              'LONG': 'BIGINT',
              'DECIMAL': 'NUMERIC',
              'Decimal': 'NUMERIC',
              'decimal': 'NUMERIC',
              'str': 'VARCHAR',
              'STR': 'VARCHAR',
              'Str': 'VARCHAR',
              'string': 'VARCHAR',
              'STRING': 'VARCHAR',
              'String': 'VARCHAR',
              'unicode': 'TEXT',
              'Unicode': 'TEXT',
              'UNICODE': 'TEXT',
              'date': 'DATE',
              'Date': 'DATE',
              'DATE': 'DATE',
              'time': 'TIME',
              'Time': 'TIME',
              'TIME': 'TIME',
              'list': 'ARRAY',
              'List': 'ARRAY',
              'LIST': 'ARRAY',
              'turple': 'COMPOSITE TYPES',
              'Turple': 'COMPOSITE TYPES',
              'TURPLE': 'COMPOSITE TYPES',
              'dict': 'hstore',
              'Dict': 'hstore',
              'DICT': 'hstore'}


def table_create(name, connection, dictionary):
    '''
    This function create table with needed
    parameters. If connection wouldn't,
    you will recieve message in terminal.
    If you wouldn't enter the dictionary,
    you will recieve message in terminal.
    '''
    try:
        cur = connection.cursor()
        try:
            parameters = 'id INT PRIMARY KEY, '
            for key in dictionary:
                parameters += f'{key} {dictionary.get(key)}, '
        except TypeError as error:
            logger.critical(f'{dictionary} is not dictionary!: {error}')
            cur.close()
            return
        parameters = parameters[:(len(parameters)-2)]
        cur.execute(f'CREATE TABLE {name}({parameters});')
        connection.commit()
        logger.info(f'Table {name} created!\n')
        cur.close()
    except AttributeError as error:
        con_type = type(connection)
        logger.critical(f'Received {con_type}. It is not connection!: {error}')
        return


def entry_insert(name, connection, id, dictionary):
        '''
        This function insert the new entry.
        It recieve name of table,
        connection, id and dictionary with values.
        If connection wouldn't,
        you will recieve message in terminal.
        If you wouldn't enter the
        dictionary, you will recieve message
        in terminal.
        '''
        try:
            cur = connection.cursor()
            try:
                parameters = 'id, '
                values = ''
                for key in dictionary:
                    parameters += f'{key}, '
                    values += f"'{dictionary.get(key)}', "

                parameters = parameters[:(len(parameters)-2)]
                values = values[:(len(values)-2)]

                cur.execute(f"INSERT INTO {name}({parameters}) VALUES({id}, {values});")
                logger.info('New entry_inserted!')
                cur.close()
                connection.commit()
            except TypeError as error:
                logger.critical(f'Invalid input data!: {error}')
                cur.close()
                return
        except AttributeError as error:
            logger.critical(f'Received {type(connection)}. It is not connection!!: {error}')
            return


def table_search(name, connection, search):
    '''
    This function search needed entry
    in table. Function recieve name,
    connection, id dictionary with names
    of fields and values. If connection wouldn't,
    you will recieve message in terminal.
    If you wouldn't enter the
    dictionary, you will recieve message
    in terminal.
    '''
    try:
        cur = connection.cursor()
        try:
                
            if(isinstance(search, int)==True):
                cur.execute(f"SELECT * FROM {name} WHERE id = {search};")
                logger.info(cur.fetchall())
                connection.commit()
                cur.close()
                return cur.fetchall()
            else:
                
                parameters = ''
                for key in search:
                        parameters += f"{key} = '{search.get(key)}' AND "
                parameters = parameters[:(len(parameters)-4)]

                cur.execute(f"SELECT * FROM {name} WHERE {parameters};")
                logger.info(cur.fetchall())
                connection.commit()
                cur.close()
                return cur.fetchall()
        except TypeError as error:
                logger.warning(f'Entry not found!: {error}')
                cur.execute(f"SELECT * FROM {name};")
                connection.commit()
                cur.close()
                return cur.fetchall()
    except AttributeError as error:
            logger.critical(f'Received {type(connection)}. It is not connection!: {error}')
            return


def entry_delete(name, connection, id, dictionary):
    '''
    This function search delete entry
    in table. Function recieve name,
    connection, id dictionary with names
    of fields and values. If connection wouldn't,
    you will recieve message in terminal.
    If you wouldn't enter the
    dictionary, you will recieve message
    in terminal.
    '''
    try:
        cur = connection.cursor()
        try:
            parameters = ''
            for key in dictionary:
                parameters += f"{key} = '{dictionary.get(key)}' AND "
            parameters = parameters[:(len(parameters)-4)]

            cur.execute(f"DELETE FROM {name} WHERE id = {id} AND {parameters};")
            logger.info('Entry deleted!')
            connection.commit()
            cur.close()
        except TypeError as error:
             logger.warning(f'Entry not found!: {error}')
             cur.close()
    except AttributeError as error:
            logger.critical(f'Received {type(connection)}. It is not connection!: {error}')
            return
    

def db_disconnect(connection):
    '''
    This function is recieved connection
    and disconnects from database. If it
    is not connection, you will recieve
    message in terminal.
    '''
    try:
          connection.close()
          logger.info('Disconnected successfully!\n')
          return
    except AttributeError as error:
         logger.critical(f'Received {type(connection)}. It is not connection!: {error}')
         return
    