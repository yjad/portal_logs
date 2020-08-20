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
    log_date_r = reverse_date(log_date)
    d = os.path.join(LOG_FILE_DIR, log_date)
    for node in os.listdir(d):
        if node[:4] == "node":
            p = os.path.join(LOG_FILE_DIR,log_date, node, "Web Application", "server.log."+log_date_r )
        else:
            p = os.path.join(LOG_FILE_DIR,log_date, node )
        log_files.append(p)
        print (p)
    return log_files


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

    print(t, dt)
    return dt


def log_2_db(log_date):
    conn, cursor = open_db()

    # check if the log file of that date is loaded?
    db_date = exec_query(cursor, "select log_date from logs limit 1")
    if db_date and db_date[0][0][:10] == reverse_date(log_date):
        close_db(cursor)
        return # already loaded
    else:
        create_tables(cursor)

    log_files = resolve_log_files(log_date)

    #out = open(out_file, "wt")
    out_error = open(nid_error_file, "wt", encoding='utf-8')

    #out.write("Date/time, Node, Line_no, Type, NID, Country, IP, service, Success, hr, min, sec\r")

    exec_cmd(cursor, "BEGIN")

    for log_file in log_files:
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
            print (node, line_no)
            try:
                res = json.loads(v)
            except:
                print (line_no, v)
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

            # outline = dt + ", " + node + "," + str(line_no) + "," + log_type + ", " +  str(res["nid"]) + \
            #           "," + res.get("Country", "No Country") + \
            #           "," + ip + \
            #           "," + service + \
            #           "," + str(res["success"]) + \
            #           "," + hr + \
            #           "," + min + \
            #           "," + sec + \
            #           "\r"
            rec = {"log_date": log_timestamp,
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










