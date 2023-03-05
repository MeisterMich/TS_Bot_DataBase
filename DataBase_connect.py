import psycopg2
import logging
# loggs, продумать исключения

logging.basicConfig(level='DEBUG')
logger = logging.getLogger()

def db_connect(database_name, self_name, self_password, self_host, self_port):
    
    '''This function is connected to database that has needed parameters and
    return connection.
    If your database wouldn't be found, you will recieve relevant message
    in terminal with exception,
    '''
    try:
        connection = psycopg2.connect(dbname=database_name, user=self_name, password=self_password, host=self_host, port=self_port)
        logger.info('Connected successfully!')
        return connection
    except Exception as error:
        logger.critical(f'Can not connected to data base error: {error}')
        return
    