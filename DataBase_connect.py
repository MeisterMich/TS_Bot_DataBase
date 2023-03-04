import psycopg2
import logging
# loggs, продумать исключения

logging.basicConfig(level='DEBUG', format='%(message)s')
logger = logging.getLogger()

def db_connect(database_name, self_name, self_password, self_host, self_port):
    
    '''Docstring:
    This function is connected to database that has needed parameters and
    return connection.
    If your database wouldn't be found, you will recieve relevant message
    in terminal with exception,
    '''
    try:
        connection = psycopg2.connect(dbname=database_name,user=self_name,password=self_password,host=self_host,port=self_port)
        logger.info(f'Connected successfully!')
        return connection
    except:
        logger.critical(f'Database not found.')
        return