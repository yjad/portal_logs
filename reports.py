from openpyxl import Workbook
from datetime import datetime, timedelta
from plogs import reverse_date

from DB import open_db, close_db, exec_query, get_last_meeting_date, get_col_names
from plogs import log_2_db

def query_to_excel(sql, file_name, header=None):
    conn, cursor = open_db()
    rows = exec_query(cursor, sql)
    close_db(cursor)

    if not header:
        header = get_col_names(conn, sql)

    wb = Workbook()
    ws = wb.active
    ws.append(header)

    for row in rows:
        ws.append(row)
    wb.save(file_name)


def login_IP_stats(log_date):
    log_2_db(log_date)

    sql = """
select count(NID) as Count_NID, 
case 
WHEN no_of_logins BETWEEN 1 and 19 Then "< 20" 
WHEN no_of_logins BETWEEN 20 and 49 Then "20-50" 
--WHEN no_of_logins BETWEEN 21 and 50 Then "20-50" 
ELSE "> 50" 
end as "logins",
count_IP_Address
FROM
(
select NID, count(log_date) as "no_of_logins", count(DISTINCT ip_address) as "count_IP_Address"
from logs
WHERE time(log_date) BETWEEN "09:30" and "10:30"
group by NID 
)
GROUP BY logins, count_IP_Address
ORDER by 3 DESC, 1 DESC
"""
    out_file = ".\\data\\Login_IP_stats-" + log_date + ".xlsx"
    query_to_excel(sql,out_file)


def calc_session_duration(nid, out):
    conn, cursor = open_db()
    cmd = f"""
    select * from logs WHERE NID = {nid} AND time(log_date) BETWEEN "09:30" and "10:30" AND service = "login"
    order by IP_address, log_date 
    """
    rows = exec_query(cursor, cmd)
    close_db(cursor)
    no_of_logins = len(rows)
    #print ("NID:", nid, "No of logins: ", len(rows))
    prev_login_date = datetime.strptime(rows[0][0],'%Y-%m-%d %H:%M:%S.%f')
    prev_ip = rows[0][5]
    session_count = -1
    total_duration = 0
    for i, row in enumerate(rows):
        if row[5] != prev_ip:
            if session_count > 0:
                out.write( nid + ":-" + " total no of logins:" +  str(no_of_logins) + " ," + prev_ip + " ,session_count: " + str(session_count)+
                          " ,Avg(sec): " + str(int(total_duration / session_count)) + "\r")
            # else:
            #     print("-->", nid, ":", prev_ip, "session_count: ", session_count, "total Duration: ",  total_duration)
            prev_ip = row[5]
            prev_login_date = datetime.strptime(row[0],'%Y-%m-%d %H:%M:%S.%f')
            total_duration = 0
            session_count = 0
        else:
            session_count += 1
            t = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S.%f')
            duration = t - prev_login_date
            total_duration += duration.total_seconds()
            prev_login_date = t
            #print (row[5], row[0], duration, duration.total_seconds(), total_duration)

    if session_count > 0:
        out.write(nid + ":-" + " total no of logins:" + str(no_of_logins) + " ," + prev_ip + " ,session_count: " +
                  str(session_count) +
                  " ,Avg(sec): " + str(int(total_duration / session_count))+ "\r")
    # else:
    #     print("-->", nid, ":", prev_ip, "session_count: ", session_count, "total Duration: ", total_duration)


def high_session_customers(log_date):

    conn, cursor = open_db()

    # check if the log file of that date is loaded?
    db_date = exec_query(cursor, "select log_date from logs limit 1")
    if db_date[0][0][:10] != reverse_date(log_date):
        log_2_db(log_date)

    sql = """
SELECT * from 
(select NID, count(log_date) as "no_of_logins", count(DISTINCT ip_address) as "count_IP_Address"
from logs
WHERE time(log_date) BETWEEN "09:30" and "10:30"
group by NID)
where no_of_logins > 2 and count_IP_address > 2
order by 2 DESC
    """

    rows = exec_query(cursor, sql)
    close_db(cursor)

    out_file = ".\\data\\High-Logins-customers-" + log_date + ".txt"
    out = open(out_file, "wt")
    out.write(f"       log data of NID as of {log_date} having logins from more than one IP between 9:30 am & 10:30 am \r\r")
    for row in rows:
        calc_session_duration(row[0], out)
        out.write("-----------------------------------------------------------------------\r")
    out.close()
