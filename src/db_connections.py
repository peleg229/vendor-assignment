"""
DB Connections
"""

import pyodbc
import pandas as pd
import psycopg2

# sql_dataSource = '172.31.19.15'
# sql_database = 'HomeC_MSCRM'
# sql_username = 'AppSQlUser'
sql_password = 'pv6%Is.W&^N0'

HOST = '172.31.71.220'
PORT = 5432
REGION = "us-east-1"
DBNAME = 'HOME365_REP'
USER = 'postgres'
PASSWORD = 'fKnHDLme37Qm^Mc'
ms_database = 'HomeC_MSCRM'
ms_user = 'AppSQlUser'
ms_pass = 'pv6%Is.W&^N0'
ms_datasource = '172.31.19.15'
ms_dev_database = 'Home365_Prd_Replica'
ms_dev_datasource = '172.31.19.15'


def importDataFromPG(query):
    print('[INFO]: Connecting to SQL')
    try:
        conn = psycopg2.connect(host=HOST,
                                database=DBNAME, user=USER, password=PASSWORD)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        raise error

    print('[INFO]: Connected to SQL...')
    print('[INFO]: Executing Query...' + query)
    data = pd.read_sql_query(query, conn)
    print('[INFO]: Query Done...')
    return data


# def create_postgres_connection(database='HOME365_REP'):
#     # config = configparser.ConfigParser()
#     # config.read('post.ini')
#     # post_host = config['postgres']['host']
#     # # post_database = config['postgres']['database']
#     # post_username = config['postgres']['username']
#     # post_password = config['postgres']['password']
#     # port = config['postgres']['port']
#     return psycopg2.connect(database=DBNAME, user=USER, password=PASSWORD, host=HOST, port=PORT)

def connectoToSQLNew(database=None):
    # if database is not None:
    #     return pyodbc.connect('DRIVER={SQL Server};SERVER='+sql_dataSource+';DATABASE='+database+';UID='+sql_username+';PWD='+ sql_password)
    return pyodbc.connect(
        'DRIVER={SQL Server};SERVER=' + ms_datasource + ';DATABASE=' + ms_database + ';UID=' + ms_user + ';PWD=' + ms_pass)


def connectoToSQL(DEV=False):
    # server = '172.31.25.110'
    if DEV == True:
        print('[INFO]: A DEV BILL')
        return pyodbc.connect(
            'DRIVER={SQL Server};SERVER=' + ms_dev_datasource + ';DATABASE=' + ms_dev_database + ';UID=' + ms_user + ';PWD=' + ms_pass)
    else:
        return pyodbc.connect(
            'DRIVER={SQL Server};SERVER=' + ms_datasource + ';DATABASE=' + ms_database + ';UID=' + ms_user + ';PWD=' + ms_pass)


"""return the reference number of the current Bill_Process inserted row"""


def get_reference_number(DEV, link_to_file):
    query = """select bill_reference from Bill_Process where fileurl = '""" + link_to_file + "'"
    return int(importDataFromSql(query=query, DEV=DEV).iloc[0]['bill_reference'])


def update_status(DEV, link_to_file):
    conn = connectoToSQL(DEV)
    cursor = conn.cursor()
    cursor.execute("""update Bill_Process set processed='unprocessed' where fileurl=?""", (link_to_file))
    conn.commit()
    conn.close()


def importDataFromSql(query, DEV=False):
    cnxn = connectoToSQL(DEV)
    sql = query
    data = pd.read_sql(sql, cnxn)
    cnxn.close()
    return data


def connectToPost():
    return psycopg2.connect(host=HOST,
                            database=DBNAME, user=USER, password=PASSWORD)


def single_insert(conn, insert_req):
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    cursor = conn.cursor()


# connectoToSQL(True)
print()