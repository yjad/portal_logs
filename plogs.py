from genericpath import isfile
import json
from operator import index
import os
from re import X

from cv2 import line
from numpy import int32
import DB as db 
from datetime import datetime, timedelta
import pandas as pd


# LOG_FILE_DIR = r".\data\ServerLogs"
# LOG_FILE_DIR = r"C:\Yahia\Home\Yahia-Dev\Python\PortalLogs-\ServerLogs\06-09-2020"
LOG_FILE_DIR = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\logs\Reservation-Server-Log"
LOG_FILE_DIR = r"C:\Users\yahia\Downloads\Reservation-Server-Log"
# LOG_FILE_DIR = r"C:\Users\yahia\Downloads\Reservation-Server-Log\27-06"
# LOG_FILE_DIR = r"C:\Users\yahia\Downloads\portal logs"
# LOG_FILE_DIR = r"C:\Yahia\Python\portal_logs\data\Serverlogs\Reservation-Server-Log"
LOG_FILE_DIR = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\logs\files Logs"

nid_error_file = r".\out\nid_error.txt"


def reverse_date(log_date):     #dd-mm-yyyy
    log_date_r = log_date[-4:] + "-" + log_date[3:5] + "-" + log_date[0:2]
    return  log_date_r


def resolve_log_files(file_path):
    log_files = []

    # if log_date:
    #     d = os.path.join(LOG_FILE_DIR, log_date)
    #     #print (d)
    # else:
    #     d = LOG_FILE_DIR

    for folder, subs, files in os.walk(file_path):
        for f in files: 
            log_files.append(os.path.join(folder,f))
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

    #print(t, dt)
    return dt


def log_2_db(log_date=None):

    log_files = resolve_log_files(log_date)
    conn, cursor = db.open_db()
    db.create_tables(cursor)

    out_error = open(nid_error_file, "wt", encoding='utf-8')

    db.exec_cmd(cursor, "BEGIN")

    for log_file in log_files:
        # check if the log file of that date is loaded?
        #"C:\Yahia\Home\Yahia-Dev\Python\PortalLogs\ServerLogs\23-06-2020\node1\Web Application\server.log.2020-06-23"
        node = log_file[24:29]
        if log_file[24:28] == "node":
            x =  f"select 1 from logs where DATE(log_date)= '{log_file[-10:]}' AND node = '{log_file[24:29]}' limit 1"
        else:
            x = f"select 1 from logs where DATE(log_date)= '{log_file[-10:]}' limit 1"
        #print (x)
        db_date = db.exec_query(cursor, x)
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
            db.insert_row(conn, cursor, "logs", rec)


        f.close()
        db.exec_cmd(cursor, "END")

    #out.close()
    out_error.close()
    db.close_db(cursor)


def log_2_db_error(log_date=None):

    search_tokens = db.query_to_list('SELECT token from error_tokens', return_header=False)

    log_files = resolve_log_files(log_date)
    # print (log_files)
    conn, cursor = db.open_db()
    # cursor.execute("DROP TABLE IF EXISTS logs")

    cursor.execute("DELETE FROM logs")

    out_error = open(nid_error_file, "wt", encoding='utf-8')

    for log_file in log_files:

        f = open(log_file, 'rt', encoding='utf-8')
        line_no = 0
        while True:
            txt = f.readline()
            if not txt:
                break
            line_no += 1
            if line_no % 100000 == 0: print (line_no)
            log_type = txt[24:29]
            if log_type in ('INFO ', 'WARN '): continue
            if log_type == 'ERROR' and txt.find('"nid"') != -1:
                p = txt.find ("{")
                v = str(txt[p:])
                try:
                    res = json.loads(v)
                except:
                    #print (line_no, v)
                    out_error.write(str(line_no) + "," + log_type + "," + v + txt)
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
                    "node": None,
                    "line_no": line_no,
                    "NID": str(res["nid"]),
                    "log_type": log_type ,
                    "country": res.get("Country", "No Country"),
                    "IP_address": ip,
                    "service": service,
                    "error_categ":str(res["success"]),
                    "error_line": None}
                db.insert_row(conn, cursor, "logs", rec)
                continue

            # Analyze other errors
            try:
                dt = datetime.strptime(txt[:19], '%Y-%m-%d %H:%M:%S')
            except:
                out_error.write(str(line_no) + "," + log_type + "," + txt)
                continue
            # search error text for tokens
            error_categ = "Undefined"
            categ_found = False
            for token in search_tokens:
                if txt.find(token[0]) != -1: 
                    error_categ = token[0]
                    txt = None
                    categ_found = True
                    break    

            rec = {"log_date": dt,
                "node": None,
                "line_no": line_no,
                "NID": None,
                "log_type": log_type ,
                "country": None,
                "IP_address": None,
                "service": None,
                "error_categ":error_categ,
                "error_line": txt}

            db.insert_row(conn, cursor, "logs", rec)

        f.close()
    conn.commit()
    out_error.close()
    db.close_db(cursor)

def log_2_df(file_path):

    # conn, cursor = db.open_db()
    # search_tokens = pd.read_sql('SELECT token, categ, prio from error_token ORDER BY prio', conn)
    # db.close_db(conn)
    search_tokens = pd.read_csv(r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\data\tokens.csv")
    # print (search_tokens)
   
    if type(file_path) == str :
        file_type = 'file'
        if os.path.isdir(file_path):
            log_files = resolve_log_files(file_path)
        else:
            log_files = [file_path]
    else:   # buffer of one or more files
        log_files = file_path
        file_type = 'buffer'

    out_error = open(nid_error_file, "wt", encoding='utf-8')
    col = ["log_date","node","line_no", "NID", "log_type" , 
                    "country", "IP_address", "service", "token", "categ", "error_line"]
    # log_pd = pd.DataFrame(columns=col)
    log_pd = pd.DataFrame()
    log_lst = []
    # print (log_pd)

    for log_file in log_files:
        if file_type == 'file':
            f = open(log_file, 'rt', encoding='utf-8')
            print (log_file)
        else:
            f= log_file
            print (log_file.name)
        
        line_no = 0
        while True:
            txt = f.readline()

            if not txt:
                break
            line_no += 1
            if file_type == 'buffer':
                txt = txt.decode('utf-8')

            # if line_no > 100: break
            if line_no % 10000 == 0: print (line_no)
            log_type = txt[24:29]
            if log_type in ('INFO ', 'WARN '): continue
            try:
                dt = datetime.strptime(txt[:19], '%Y-%m-%d %H:%M:%S')
            except:
                out_error.write('***Invalid record format***, '+ str(line_no) + ", ERROR," + str(txt))
                continue    # Invalid record format, ignore rest of parsing
            rec = None
            if log_type == 'ERROR' and txt.find('"nid"') != -1:
                rec = parse_nid_rec(txt, line_no, out_error, dt)
                # pass
            else:
                rec  = parse_tech_rec(txt, line_no, out_error, search_tokens, dt)
                # pass

            if not rec: continue    # invalid line, skip it
  
            log_lst.append(rec)      

    out_error.close()
    log_pd = pd.DataFrame(log_lst, columns = col)
    return log_pd



def parse_nid_rec(txt, line_no, out_error, log_timestamp):

    p = txt.find ("{")
    v = str(txt[p:])
    try:
        res = json.loads(v)
        # dt = str(res["datetime"])
        service = str(res["service"])
        # log_timestamp= get_timestamp(dt, service)
        ip = res.get("Address", "No IP")
        if ip != "No IP":
            ipa = ip.split(",")
            if len(ip) > 0:
                ip = ipa[0]
        # if res['success']:
        #     token = 'Login Success' if service == 'login' else 'Logout Success'
        # elif not res['success']: 
        #     token = 'Login Failed' if service == 'login' else 'Logout Failed'
        # else:
        #     token = service
        x = service + " " + str(res['success'])
        match x:
            case 'login True':  token = 'Logins'
            case 'login False':  token = 'Failed Logins'
            case 'logout True':  token = 'Logout'
            case _: token = x

        nid = str(res["nid"])
        cntry = res.get("Country", "No Country")
    except Exception as e:
        ip = None
        nid = None
        cntry = None
        if v.find('"success":false'):   # handle invalid JSON format for some records
            token = 'Failed Logins' 
            service = 'login'
        else:   # unhandled line
            # print ('***', line_no, v)
            out_error.write('NID '+str(line_no) + ", ERROR," + str(e) +"||"+ v)
            token = "Unclassified"
            service = 'Unclassified'

    log_type = 'ERROR'
    error_categ = 'user'
    rec_lst = [log_timestamp, None, line_no, nid, log_type, cntry, ip, service, token, error_categ, None]
    return rec_lst

def parse_tech_rec(txt, line_no, out_error, search_tokens, dt):
    
    # search error text for tokens
    error_categ = "Undefined"
    # print (search_tokens)
    # token_found = False
    for token in search_tokens.itertuples():
        if txt.find(token[1]) != -1:
            # print (token, "------->", token[1]) 
            error_token = token[1]
            error_categ = token[2]
            txt = None
            break    

    if txt: #categ not found
        # print (f"{line_no} - txt is not null, ", txt)
        out_error.write("Unclassified: "+txt)
        error_token = "Unclassified"
        error_categ = 'Unclassified'
        #  return None
    log_type = 'ERROR'
    rec_lst = [dt, None, line_no, None, log_type, None, None, None, error_token, error_categ, txt]

    return rec_lst


def update_prio(log_df):

    print (log_df.columns)

    x = log_df[['token', 'line_no']].fillna('x').\
            groupby(['token'],as_index = False).count().sort_values(by='line_no', ascending=False)
    
    x.to_csv('.\\out\\token prio.csv', index = False)
    conn, cursor = db.open_db()
    db.exec_cmd(cursor, "UPDATE error_token set prio = 99")
    i = 0
    for r in x.iterrows():
        cmd = f'UPDATE error_token set prio = {i} WHERE token = "{r[1][0]}"'
        # print (cmd)
        db.exec_cmd(cursor, cmd)
        i = i + 1

    conn.commit()
    db.close_db(cursor)


def append_stats(log_df):

    conn, cursor = db.open_db()

    log_df['dt'] = pd.to_datetime(log_df['log_date']).dt.date  
    x = log_df[['dt', 'token','categ', 'line_no']].fillna('x').groupby(['dt', 'token','categ'],as_index = False).count()
    x.to_sql('log_stats', conn, if_exists = 'append')

    db.close_db(cursor)



def print_stats(log_df):

    x = log_df[log_df.categ== 'user']
    
    x['dt'] = pd.to_datetime(x['log_date']).dt.date  
    x = x[['dt', 'token','categ', 'line_no']].fillna('x').groupby(['dt', 'token','categ'],as_index = False).count()
    x.to_csv('.\\out\\log_stats.csv', index = False)
   
    log_df.to_csv('.\\out\\log_df.csv', index = False)
    print (x)
   
def export_email_quota_graph():
    conn, cursor = db.open_db()
    cmd = """
select * from 
(select dt, sum(line_no) '# logins' from log_stats where token = 'Logins' group by dt)
left join
(select dt, sum(line_no) '# email quota errors' from log_stats where token = 'MailSendException' group by dt)
using (dt)
"""
    df = pd.read_sql(cmd, conn)
    cursor.close()

    fig = df.plot(x='dt', y=['# logins', '# email quota errors'], title = 'Reservation Portal Logins vs email quota error', grid=True,
            xlabel = 'Date', ylabel = '# of customers', figsize = (7,5)).get_figure()

    # fig.show()
    fig.savefig(r'.\out\email quota.jpg')
    return df

# summerize log file(s). Load logins summary into DB and save summary csv file with  
def summerize_portal_logs(fpath, load_db=True):
    log_df = log_2_df (fpath)

    if load_db:
        # get dates in files and delete exisiting stats for this date
        # log_dates = pd.to_datetime(log_df['log_date']).dt.strftime('%Y-%m-%d').value_counts().sort_values(ascending=False)#.index[0]
        log_dates = pd.to_datetime(log_df['log_date']).dt.strftime('%Y-%m-%d').drop_duplicates()
        log_dates_str = '"' +'","'.join(list(log_dates))+ '"'
        conn, cursor = db.open_db()
        db.exec_cmd(cursor, f"DELETE FROM log_stats WHERE dt in ({log_dates_str})")

        log_df['dt'] = pd.to_datetime(log_df['log_date']).dt.date  
        x = log_df[['dt', 'token','categ','line_no']].fillna('x').groupby(['dt', 'token','categ'],as_index = False).count()
        x.to_sql('log_stats', conn, if_exists = 'append')
        db.close_db(cursor)
    else:
        log_df['dt'] = pd.to_datetime(log_df['log_date']).dt.date  
        x = log_df[['dt', 'token','categ', 'line_no']].fillna('x').groupby(['dt', 'token','categ'],as_index = False).count().\
            sort_values(by=['dt', 'categ', 'line_no'], ascending=False)
        # print (x)

    out_path = r'.\out\log csv'
    if type(fpath) == str:
        out_file_path = os.path.join(out_path, datetime.today().strftime('%Y-%m-%d')+ '_' + os.path.basename(fpath)+  '.csv')
    else:
        base_name= os.path.basename(fpath[0].name)
        out_file_path = os.path.join(out_path, datetime.today().strftime('%Y-%m-%d')+ '_' + base_name+ '.csv')
    log_df.to_csv(out_file_path, index=False)
   
def plot_email_quota_error():
    df = export_email_quota_graph().fillna(0)
    fig = df.plot(x='dt', y=['# logins', '# email quota errors'], title = 'Reservation Portal Logins vs email quota error', grid=True,
            xlabel = 'Date', ylabel = '# of customers', figsize = (10,5)).get_figure()
    return fig

def plot_failed_logins(filename):
    df = pd.read_csv(filename, low_memory=False)
    x = df[['token', 'dt', 'line_no']].loc[df.token.isin(['Logins', 'Failed Logins', 'MailSendException'])].groupby(['dt','token']).count().reset_index()
    if len(df.dt.unique()) > 1:
        fig = x.pivot(index='dt', columns='token', values = 'line_no').plot(kind='line').get_figure()
    else:
        fig = x.pivot(index='dt', columns='token', values = 'line_no').plot(kind='bar').get_figure()
    return fig

    
def display_login_cntry(filename):
    df = pd.read_csv(filename, low_memory=False)
    cntry = pd.read_csv('.\\data\\cntry.csv', low_memory=False)
    df.rename (columns={'line_no':'Count'}, inplace = True)
    
    df.country.fillna('not logged', inplace=True)
    dts = df.dt.unique()
    dts_from = dts[0]
    dts_to=dts[-1]
    # x = df.loc[df.token =='Logins', ['country', 'Count']][df.country != 'EG'].dropna().groupby(['country']).count()
    x = df.loc[df.token =='Logins', ['country', 'Count']].groupby(['country']).count()
    df = x.join(cntry.set_index('Alpha-2 code'), how = 'left').sort_values(['Count'], ascending = False).reset_index()#.drop(columns='country')
    return df, dts_from, dts_to

def display_log_summary(filename):
    df  = pd.read_csv(filename, low_memory=False)
    # dfx= df[['dt', 'token', 'line_no']][df.categ == 'user'].groupby(['dt','token']).count().sort_values('line_no', ascending=False).rename(columns={'line_no':'Count'})
    dts = df.dt.unique()
    dts_from = dts[0]
    dts_to=dts[-1]
    dfx = df[['dt', 'token', 'line_no']][df.categ == 'user'].groupby(['dt','token']).count().sort_values('line_no', ascending=False).rename(columns={'line_no':'Count'}).reset_index()
    
    # dfx = dfx.fillna(0).astype(int32)
    if len(dts) > 1:
        fig = dfx.pivot(index='dt', columns='token', values = 'Count').plot(kind = 'line').get_figure()
    else:
        fig = dfx.pivot(index='token', columns='dt', values = 'Count').plot(kind='bar').get_figure()
    return fig, dts_from, dts_to

if __name__ == "__main__":
    # log_df = log_2_pd()
    # print ('Update Prio ....')
    # # update_prio(log_df)
    # print ('write log.csv ....')
    # log_df.to_csv(r'.\out\log_errors.csv')
    # print ('Print Stats ....')
    # print_stats(log_df)   
    # append_stats(log_df)

    # fpath = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\logs\2022-06-28"
    # load_error_files(fpath)
    # export_email_quota_graph()

    # fpath = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\logs\2022-06-28"
    # fpath = r"C:\Yahia\Home\Yahia-Dev\Python\PortalLogs-\ServerLogs\16-06-2020\node1\Web Application\server.log.2020-06-16"
    # fpath = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\logs\sherif28.txt"
    # fpath = r"C:\Users\yahia\Downloads\Reservation-Server-Log\files Logs"
    # fpath = r"C:\Users\yahia\Downloads\Reservation-Server-Log"
    # fpath = "C:\Yahia\Python\portal_logs\ServerLogs\sherif28.txt"
    # fpath = r"C:\Yahia\Python\portal_logs\ServerLogs\files Logs"
    # fpath = "C:\Yahia\Python\portal_logs\ServerLogs\Reservation-Server-Log"
    fpath = r"C:\Yahia\Python\portal_logs\ServerLogs\26-07-2020\server.log.2020-07-26"
    fpath = r"C:\Yahia\Python\portal_logs\ServerLogs\06-09-2020"
    # export_email_quota_graph()
    # summerize_portal_logs(fpath, load_db=True)