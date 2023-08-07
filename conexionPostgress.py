import psycopg2
import os

def conexionPostgres():
    conexion = ''
    try:
        host     = os.environ.get('POSTGRESQL_HOST')
        username = os.environ.get('POSTGRESQL_USER')
        password = os.environ.get('POSTGRESQL_PASSWORD')
        database = os.environ.get('POSTGRESQL_DB')
        conexion = psycopg2.connect(host = host,database = database,user = username,password = password)        
        conexion.autocommit = True

    except Exception as e:
        print("Error", e)

    return conexion





