import psycopg2
import logging

logging.basicConfig(level='DEBUG')
logger = logging.getLogger()

#dictionary = {'name': 'VARCHAR NO NULL', 'date': 'DATE'}
#ожидалось (тип), ожидалось (тип), написать ф-ю проверки словаря
def validator(string):
    return isinstance(string, str)

