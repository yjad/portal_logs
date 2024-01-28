import io
import os
import zipfile, tarfile
from datetime import datetime#, timedelta
import pandas as pd
# from plogs import yield_zip_file
from rarfile import RarFile


def zip_log_to_df(zip_file):
    if type(zip_file) == str :
        file_type = 'file'
        if os.path.isdir(zip_file):
            # log_files = resolve_log_files(file_path)
            pass
        else:
            log_files = [zip_file]
            file_type = os.path.splitext(log_files[0])[1]
            
    else:   # buffer of one or more files
        file_type = os.path.splitext(zip_file.name)[1]
        # print (log_file.name)

    log_lst = []
    # print (zip_file, file_type)
    # with zipfile.ZipFile(zip_file, "r") as zf:
        # for fname in zf.namelist():
            # f = zf.open(fname) 
    zfiles = yield_rar_file(zip_file)
    for f in zfiles:
        line_no = 0
        while True:
            txt = f.readline()
            if not txt: break # end of file
            line_no += 1

            # if line_no > 100000: break
            # if line_no < 100000000: continue
            # if line_no % 10000 == 0: print (line_no)
            if line_no % 10000 == 0: print (line_no)
            
            txt = txt.decode('utf-8')
            rec= parse_db_log_line(line_no, txt)
            log_lst.append(rec)  

        col = ['line_no', 'timestamp', 'n', 'query_type', 'cmd', 'proc_tbl', 'params', 'logLine']
        log_pd = pd.DataFrame(log_lst, columns = col)
        print('Done ...')
        return log_pd


def parse_db_log_line(line_no, log_line):
    txt = log_line.replace('\n', '').replace ('\t', ' ')
    txt_lst = txt.split()
    txt_len = len(txt_lst)
    # print (txt[:19])
    dt = datetime.strptime(txt[:19], '%Y-%m-%dT%H:%M:%S')
    try:
        n= int(txt_lst[1])      #int(txt[35:39])
        query_type=txt_lst[2]   #txt_lst[39:45]
        if txt_len == 3:
            cmd = txt_lst[2]
            query = ' '.join(txt_lst[2:])
        elif txt_len == 4:
            cmd= txt_lst[3]
            query = ' '.join(txt_lst[3:])
        else:
            cmd = txt_lst[3]       
            query = ' '.join(txt_lst[3:])
    except:
        print (txt_lst, txt_len)
        cmd='XXXX'

    proc_tbl = ''
   
    params = ''
    if cmd.upper() == 'CALL':
        proc_tbl = txt_lst[4]   #cmd_lst[1]
        # print (query)
        p = query.find('(')
        if p != -1:
            params = query[p:]
            proc_tbl = query[5:p]
            # print (proc_tbl, params)

    elif cmd.upper() == 'SELECT':
        try:
            proc_tbl = txt_lst[txt_lst.index('FROM')+1]
        except:
            # print (query)
            pass    #SELECT @com_mysql_jdbc_outparam_log_id

    return [line_no, dt, n, query_type, cmd, proc_tbl, params, log_line]


def dblog_from_to(csv_fn, from_dttm,to_dttm):
    df1 = pd.read_csv(csv_fn, parse_dates=True)
    
    from_time = pd.to_datetime(from_dttm)        # ('2022-12-27 10:00')
    to_time= pd.to_datetime(to_dttm)             # ('2022-12-27 11:00')

    df = df1.loc[pd.to_datetime(df1.tmstamp) >= from_time].loc[pd.to_datetime(df1.tmstamp) <= to_time]
    
    csv_fn= os.path.basename(csv_fn).split('.')[0]
    df.to_csv(f'./out/dblog_from-to_{csv_fn}.csv', index=False )


def dblog_proc_params(dblog_csv_fn, proc_name):
    df = pd.read_csv(dblog_csv_fn, parse_dates=True, usecols=['params','proc_tbl'])
    
    tbl = df.loc[df.proc_tbl == proc_name]  
    nparams = len(tbl.iloc[0,1].split(','))
    df = pd.DataFrame()
    t = tbl.params.str.split(',')
    for i in range(nparams) :
        df[f"P{i+1}"] = t.apply(lambda x: x[i].replace("'",""))  
    csv_fn= os.path.basename(dblog_csv_fn).split('.')[0]
    df.to_excel(f'./out/dblogProcParams_{csv_fn}.xlsx', index=False )

def dblog_by_query_type(dblog_csv_fn, query_type):
    df = pd.read_csv(dblog_csv_fn, parse_dates=False)
    
    tbl = df.loc[df.query_type.str.upper() == query_type.upper()]  
    csv_fn= os.path.basename(dblog_csv_fn).split('.')[0]
    tbl.to_csv(f'./out/dblogByQueryType-{csv_fn}.csv', index=False )

def dblog_by_cmd(df, cmd):
    # df = pd.read_csv(dblog_csv_fn, parse_dates=False)
    
    tbl = df.loc[df.cmd.str.upper() == cmd.upper()]  
    return tbl


def yield_rar_file(rar_filename):

    scan_data = pd.DataFrame()
    if type(rar_filename) == str:
        rar_files = RarFile(rar_filename)
    else:   # UploadedFile object
        try:
            rar_files = RarFile(io.BytesIO(rar_filename.read()))
        except: # home PC does not find le32
            # LOG_DIR = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Projects\Realestate Reservation Portal\5- Operation\Incident reports\change land-no\DB logs"
            LOG_DIR = r"C:\Users\yahia\OneDrive - Data and Transaction Services\DTS-data\PortalLogs\DB-Log"
            rar_files = RarFile(os.path.join(LOG_DIR,rar_filename.name))

    for f in rar_files.infolist():
        print (f.filename)
        if f.is_dir():
            print ("directory:", f.filename)
            continue
        
        try:
            rar_file = RarFile.open(rar_files, f.filename)
        except Exception as err:    # io.UnsupportedOperation:
            print (f.filename, f'err opening file: {err}')
            continue

        yield rar_file        


if __name__ == '__main__':
    # filename = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Projects\Realestate Reservation Portal\5- Operation\Incident reports\change land-no\DB logs\hdbbe1c-27.rar" 
    # zip_log_to_df(filename)
    # --------------------------------------
    # csv_fn = "C:\Yahia\Python\portal_logs\out\db_log.csv"
    # from_dttm = '2022-12-27 10:00'
    # to_dttm = '2022-12-27 11:00'
    # dblog_from_to(csv_fn, from_dttm,to_dttm)
    # --------------------------------------

    # dblog_csv_fn = r"C:\Yahia\Python\portal_logs\out\dblog_from-to_db_log.csv"
    # dblog_proc_params(dblog_csv_fn, 'reserveLand')
    
    dblog_csv_fn = "C:\Yahia\Python\portal_logs\out\db_log.csv"
    # dblog_by_query_type(dblog_csv_fn, 'Connect')

    dblog_by_cmd(dblog_csv_fn, 'update')

