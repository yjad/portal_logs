import sqlite3
from time import time
#from config import config
MEETING_TABLE = "meetings"
ATTENDEES_TABLE = "attendees"
DB_FILE_NAME = r".\data\logs.sqlite"

PRINT_INSERT_ERROR = True

def open_db():
    connection = sqlite3.Connection(DB_FILE_NAME)
    cursor = connection.cursor()
    return connection, cursor


def create_tables(cursor):
    #cmd = "DROP TABLE IF EXISTS logs"
    #cursor.execute(cmd)

    cmd = f'CREATE TABLE IF NOT EXISTS logs' \
          '(log_date TEXT,' \
          'node TEXT,' \
          'line_no INTEGER ,' \
          'NID TEXT ,' \
          'log_type INTEGER,' \
          'country	TEXT,' \
          'IP_address TEXT,' \
          'service TEXT,' \
          'success TEXT)'
    cursor.execute(cmd)

    return


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
            print(traceback.format_exception(exc_type, exc_value, exc_tb))
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


