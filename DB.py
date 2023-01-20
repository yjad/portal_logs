import sqlite3
from time import time
import sys
import pandas as pd

#from config import config
MEETING_TABLE = "meetings"
ATTENDEES_TABLE = "attendees"
DB_FILE_NAME = r"..\data\portal_logs.sqlite"
# DB_FILE_NAME = r"C:\Yahia\Home\Yahia-Dev\Python\portal_logs\data\rep_land.sqlite"
# DB_FILE_NAME = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\data\logs.sqlite"

PRINT_INSERT_ERROR = True

def open_db():
    try:
        connection = sqlite3.Connection(DB_FILE_NAME)
        cursor = connection.cursor()
        return connection, cursor
    except Exception as e:
        print ('DB not found: ', DB_FILE_NAME)
    # create_tables(cursor)
    


# def create_tables(cursor):
    
#     cmd = f'CREATE TABLE IF NOT EXISTS logs' \
#           '(log_date TEXT,' \
#           'node TEXT,' \
#           'line_no INTEGER ,' \
#           'NID TEXT ,' \
#           'log_type INTEGER,' \
#           'country	TEXT,' \
#           'IP_address TEXT,' \
#           'service TEXT,' \
#           'error_categ TEXT,'\
#           'error_line TEXT)'
#     cursor.execute(cmd)

#     return


def close_db(cursor):
    cursor.close()

def exec_cmd(cursor, cmd):
    cursor.execute(cmd)


def insert_row(conn, cursor, table_name, rec):

    keys = ','.join(rec.keys())
    question_marks = ','.join(list('?' * len(rec)))
    values = tuple(rec.values())
    try:
        cursor.execute('INSERT INTO ' + table_name + ' (' + keys + ') VALUES (' + question_marks + ')', values)
        return 0
    except sqlite3.Error as er:
        if PRINT_INSERT_ERROR:
            print('SQLite error: %s' % (' '.join(er.args)))
            print("Exception class is: ", er.__class__)
            print('SQLite traceback: ')
            exc_type, exc_value, exc_tb = sys.exc_info()
            print(f"{exc_type} \n {exc_value}, {exc_tb}")
            # print(traceback.format_exception(exc_type, exc_value, exc_tb))
        return -1

def insert_row_log(conn, cursor, rec):
    return insert_row(conn, cursor, "logs", rec)


def exec_query(cursor, cmd):
    try:
        cursor.execute(cmd)
    except:
        return None # table does not exist

    return cursor.fetchall()



def get_last_meeting_date():
    conn, cursor = open_db()
    cursor.execute('SELECT start_time from meetings order by start_time DESC LIMIT 1')
    rows = cursor.fetchall()
    close_db(cursor)
    return rows[0][0][:10]


def get_col_names(conn, sql):
    get_column_names = conn.execute(sql + " limit 1")
    col_name = [i[0] for i in get_column_names.description]
    return col_name

def query_to_list(cmd, return_header = True):
    conn, cursor = open_db()
    rows = exec_query(cursor, cmd)
    if return_header:
        if cmd.upper().find("LIMIT") != -1:   # if command have "Limit clause, dont return header
            header = []
        else:
            header = get_col_names(conn, cmd)
        close_db(cursor)
        return header, rows
    else:
        close_db(cursor)
        return rows

def query_to_pd(cmd, table:bool=None): 
    
    conn, _ = open_db()
    if table == None:
        if len(cmd.split()) == 1: # # if one word, its table, else cmd is a query
            table = True
        else:
            table = False
    if table:
        df = pd.read_sql(f"SELECT * FROM {cmd}", conn)
    else:
        df = pd.read_sql(cmd, conn)
    conn.close()
    return df
