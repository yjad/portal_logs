import json
import os
from DB import insert_row, open_db, close_db, create_tables, exec_cmd, exec_query
from datetime import datetime, timedelta

LOG_FILE_DIR = r".\ServerLogs"


nid_error_file = r".\data\nid_error.txt"


def reverse_date(log_date):     #dd-mm-yyyy
    log_date_r = log_date[-4:] + "-" + log_date[3:5] + "-" + log_date[0:2]
    return  log_date_r


def resolve_log_files(log_date):
    log_files = []

    if log_date:
        d = os.path.join(LOG_FILE_DIR, log_date)
        #print (d)
    else:
        d = LOG_FILE_DIR

    for folder, subs, files in os.walk(d):
        if files and files[0][:11] == "server.log.":
            log_files.append(os.path.join(folder,files[0]))
            #print (os.path.join(folder,files[0]))
    return log_files

    # for node in os.listdir(d):
    #     if node[:4] == "node":
    #         p = os.path.join(LOG_FILE_DIR, log_date, node, "Web Application", "server.log." + reverse_date(log_date))
    #     else:
    #         p = os.path.join(LOG_FILE_DIR, node) # one node log
    #     log_files.append(p)
    #     print(p)
    # else: # load all logs in the folder
    #     for log_date_dir in os.listdir(LOG_FILE_DIR):
    #         for node in os.listdir(os.path.join(LOG_FILE_DIR, log_date_dir)):
    #             if node[:4] == "node":
    #                 p = os.path.join(LOG_FILE_DIR, log_date, node, "Web Application", "server.log." + log_date_r)
    #             else:
    #                 p = os.path.join(LOG_FILE_DIR, node)
    #             log_files.append(p)
    #             print(p)
    #
    # return log_files


def get_timestamp(t, service):

    if service == "confirmReservation" or service == "confirmLandReservation":
        if len(t) == len ("2020-06-16T10:02:42"): # no second fraction
            dt = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
        else:
            # Date Format: 2020-06-16T10:02:42.313
            dt = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%f')
    else:
        # date format: "2020_07_26_00_00_30_394AM"
        dt = datetime.strptime(t, '%Y_%m_%d_%H_%M_%S_%f%p')

    #print(t, dt)
    return dt


def log_2_db(log_date=None):

    log_files = resolve_log_files(log_date)
    conn, cursor = open_db()
    create_tables(cursor)

    out_error = open(nid_error_file, "wt", encoding='utf-8')

    exec_cmd(cursor, "BEGIN")

    for log_file in log_files:
        # check if the log file of that date is loaded?
        #"C:\Yahia\Home\Yahia-Dev\Python\PortalLogs\ServerLogs\23-06-2020\node1\Web Application\server.log.2020-06-23"
        node = log_file[24:29]
        if log_file[24:28] == "node":
            x =  f"select 1 from logs where DATE(log_date)= '{log_file[-10:]}' AND node = '{log_file[24:29]}' limit 1"
        else:
            x = f"select 1 from logs where DATE(log_date)= '{log_file[-10:]}' limit 1"
        #print (x)
        db_date = exec_query(cursor, x)
        if db_date:
            continue  # already loaded

        f = open(log_file, 'rt', encoding='utf-8')
        p = log_file.find("node")
        node = log_file[p:p+5]
        line_no = 0
        while True:
            txt = f.readline()
            if not txt:
                break
            line_no += 1
            if txt.find('"nid"') == -1: # not a log record
                continue
            log_type = txt[24:29]
            p = txt.find ("{")
            v = str(txt[p:])
            #print (line_no, v)
            #print (node, line_no)
            try:
                res = json.loads(v)
            except:
                #print (line_no, v)
                out_error.write(node + "," + str(line_no) + "," + log_type + "," + v)
                continue

            dt = str(res["datetime"])
            service = str(res["service"])
            log_timestamp= get_timestamp(dt, service)
            ip = res.get("Address", "No IP")
            if ip != "No IP":
                ipa = ip.split(",")
                if len(ip) > 0:
                    ip = ipa[0]

            rec = {"log_date": log_timestamp,
                   "node": node,
                   "line_no": line_no,
                   "NID": str(res["nid"]),
                   "log_type": log_type ,
                   "country": res.get("Country", "No Country"),
                   "IP_address": ip,
                   "service": service,
                   "success":str(res["success"])}
            insert_row(conn, cursor, "logs", rec)


        f.close()
        exec_cmd(cursor, "END")

    #out.close()
    out_error.close()
    close_db(cursor)










