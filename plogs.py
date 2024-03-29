import json
import os
# import io
from pickletools import int4
from io import StringIO
# from sqlite3 import SQLITE_CREATE_INDEX
import numpy as np
# import zipfile, tarfile
# from csv import QUOTE_ALL
import plog2
import streamlit as st
# import py7zr
# import bz2
# from rarfile import RarFile
from uncompress import yield_zip_file


SUMMARY_FOLDER = r"C:\Users\yahia\OneDrive - Data and Transaction Services\DTS-data\PortalLogs\summary" 


# from numpy import int32
import DB as db 
from datetime import datetime#, timedelta
import pandas as pd
import country_codes
import tokens_data


Exception_File = ''
# CSV_PATH = r'.\data\log csv'
nid_error_file = r".\out\nid_error.txt"   
All_df = pd.DataFrame()
Cntry = country_codes.Cntry
search_tokens = tokens_data.Tokens 

def resolve_log_files(file_path):
    log_files = []

    for folder, subs, files in os.walk(file_path):
        for f in files: 
            log_files.append(os.path.join(folder,f))
    return log_files




# Process one zip file
def zip_log_to_df(zip_file):
    out_error = open(nid_error_file, "wt", encoding='utf-8')
    land_col = ["log_date","node","line_no", "NID", "log_type" , 
                    "country", "IP_address", "service", "token", "categ",  'task_id', 'project_id',"error_line",
                    'Gov','Land_ID', 'City','Region','District','Sub_District','Land_No','land_size','excellence_ratio','checksum', 'NA1']
                    # 'Gov','City','Region','District','Sub_District','Land_No','land_size','excellence_ratio','checksum', 'NA1', 'NA2']
    unit_col = ["log_date","node","line_no", "NID", "log_type" , 
                    "country", "IP_address", "service", "token", "categ",'project_id', 'task_id',  "error_line",
                    'Gov', 'City','Region','District','Sub_District','Floor_No','building_no','Unit_No','Unit_Model', 'Unit_ID', 'checksum']
    # log_pd = pd.DataFrame(columns=col)
    placeholder = st.empty()
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
        # print (zip_file.name)

    log_pd = pd.DataFrame()
    log_lst = []
    # proj_id_lst = []
    project_type = None
    placeholder.write("Unzip log file ...")
    zfiles = yield_zip_file(zip_file, file_type)
    placeholder.write("Reading log file ...")
    for f, file_size in zfiles:

        # # line_count = sum(1 for _ in f)
        # # f.seek(0)
        # st.write('Get file size ...')
        if not file_size:   # file size is retruned from tar, calc it
            f.seek(0,2) # end
            file_size = f.tell()
            # line_count = int(file_size/145)   # avrage 145 bytes/line
            f.seek(0,0)     # set pointer to beginning of file
        # st.write('End of get file size ...')
        line_count = int(file_size/145)   # avrage 145 bytes/line
        # my_prog_bar = st.progress(0, text="")
       
        # percent_complete = 0
        line_no = 0
        placeholder.write(f"{line_no:,}/{line_count:,} - {line_no/line_count:.2%}")
        while True:
            txt = f.readline()
            if not txt: break
            line_no +=1

            # if line_no > 50000: break
            # if line_no < 100000000: continue
            # if line_no % 10000 == 0: print (line_no)
            if line_no % 40000 == 0: 
                # percent_complete =  int(line_no/line_count*100)
                # my_prog_bar.progress(percent_complete, text=f"Operation in progress. Please wait- {line_no}/{line_count}" )
                placeholder.write(f"{line_no:,}/{line_count:,} - {line_no/line_count:.2%}")
            
            if type(txt) == bytes: txt= txt.decode('utf-8')
            
            try:
                dt = datetime.strptime(txt[:19], '%Y-%m-%d %H:%M:%S')
                log_type = txt[24:29]
                # print (log_type)
            except:
                out_error.write('***Invalid record format***, '+ str(line_no) + ", ERROR," + str(txt))
                continue    # Invalid record format, ignore rest of parsing
            rec = None
            # if txt.find('WebRequestInterceptor') != -1: #('"nid"') != -1: #log_type == 'ERROR' and , time optimization
            if 'WebRequestInterceptor' in txt: # time optimization
                # task_id = int(txt[99:103]) # task_id
                task_id = None
                project_id = None
                # ----------- Debug tech rec only -----
                rec, rec_project_type = parse_nid_rec(txt, line_no, out_error, dt, log_type, project_id, task_id)
                if not project_type and rec_project_type:   # set it once in the file
                    project_type = rec_project_type 
                #--------------
            else:
                if log_type in ('INFO ', 'WARN '): 
                    # if log_type == 'INFO ' and txt[106:118] == 'Project ID: ': 
                        # proj_id_lst.append([line_no, int(txt[118:]), int(txt[100:104])]) # project_id & task_id
                    continue # skip info for tech errors, keep only lines for project-id
                
                rec  = plog2.parse_tech_rec(txt, line_no, out_error, dt, log_type)
            if not rec: continue    # invalid line, skip it
            # print ('------------', rec)
            log_lst.append(rec) 
        # my_prog_bar.progress(100, text=f"Operation in progress. Please wait- {line_no}/{line_count}" )
        placeholder.write(f"{line_no:,}/{line_count:,} - {line_no/line_count:.2%}")


    out_error.close()
    if project_type == 'confirmLandReservation':  
        log_pd = pd.DataFrame(log_lst, columns = land_col)
    else:
        try:
            log_pd = pd.DataFrame(log_lst, columns = unit_col)
        except ValueError: # no-reservation day, no details in all log. use mini column list with no details
            # pd.DataFrame(log_lst).to_excel('./out/test.xlsx')
            log_pd = pd.DataFrame(log_lst, columns = land_col[:13])

    # log_pd.to_csv('./out/log_pd.csv', index=False)
    # pd.DataFrame(proj_id_lst, columns=['line_no', 'project_id', 'task_id']).to_csv('./out/proj_id_lst.csv')
    return log_pd


def summerize_exception_file(uploaded_file):
    col = ["log_date","node","line_no", "NID", "log_type" , 
                    "country", "IP_address", "service", "token", "categ", "error_line",
                    'Gov','City','Region','District','Sub_District','Land_No','land_size','excellence_ratio','checksum']
    log_pd = pd.DataFrame()
    log_lst = []
    # print(file_path)
    
    stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
    # f = open(file_path, 'rt', encoding='utf8')
    while True:
        txt = stringio.readline()
        if not txt: break # end of file
        line_no= txt.split(',')[0]
        txt = txt[len(line_no)+1:]
        log_type = txt[24:29]
        # print (log_type, txt)
        try:
            dt = datetime.strptime(txt[:19], '%Y-%m-%d %H:%M:%S')
        except:
            print('***Invalid record format***, '+ str(line_no) + ", ERROR," + str(txt))
            continue    # Invalid record format, ignore rest of parsing
        # rec = None
        
        # if txt.find('WebRequestInterceptor') != -1: #('"nid"') != -1: #log_type == 'ERROR' and , time optimization
        if 'WebRequestInterceptor' in txt: #time optimization
            rec = parse_nid_rec(txt, line_no, None, dt, log_type)
        else:
            if log_type in ('INFO ', 'WARN '): continue # skip info for tech errors
            rec  = parse_tech_rec(txt, line_no, None, dt, log_type)

        if not rec: continue    # invalid line, skip it
        log_lst.append(rec)  

    log_pd = pd.DataFrame(log_lst, columns = col)
    d= pd.to_datetime(log_pd['log_date']).dt.date.unique()[0]
    # print (d)
    # log_pd = d
    file_name = f"log summary-exception-{d}.zip"
    out_file_path = os.path.join(SUMMARY_FOLDER, file_name)
    # log_pd = log
    log_pd.to_csv(out_file_path, index_col=['line_no'],  
                  compression={'method': 'zip', 'archive_name': f"log summary-exception-{d}.csv"})
    
def data_cleansing(txt):
    return txt.replace ('""', '"')  # replace double quote in text to a single

def parse_nid_rec(txt, line_no, out_error, log_timestamp, log_type, project_id, task_id):
    global Exception_File

    txt = data_cleansing(txt)
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
        x = service + " " + str(res['success'])
        match x:
            case 'login True':  token = 'Logins'
            case 'login False':  token = 'Failed Logins'
            case _: token = x

        nid = str(res["nid"])
        cntry = res.get("Country", "No Country")
        # print ("===============",nid)

    except Exception as e:
        # print (txt)
        # with open(Exception_File, 'at', encoding='utf8')as f:
        #    f.write(f"{log_timestamp},{line_no},{txt}")  # append line
        ip = None
        nid = None
        cntry = None
        if v.find('"service":"login"') != -1:   # handle invalid JSON format for some records
            token = 'Invalid format' 
            service = 'login'
        else:   # unhandled line
            # print ('***', line_no, v)
            with open(Exception_File, 'at', encoding='utf8')as f:
                f.write(f"{line_no},{txt}")  # append line
            return None, None
            # out_error.write('NID '+str(line_no) + ", ERROR," + str(e) +"||"+ v)
            # token = "Unclassified"
            # service = 'Unclassified'

    # log_type = 'ERROR'
    error_categ = 'user'
    rec_lst = [log_timestamp, None, line_no, nid, log_type, cntry, ip, service, token, error_categ, project_id, task_id, None]
    if service in ('confirmReservation', 'confirmLandReservation'):
        project_type = service
        # rec_lst += list(res.get('details').values())
        det_dict = res.get('details')
        det_lst = list(det_dict.values())
        if service == 'confirmLandReservation' and det_dict.get('Land_ID') == None: # Land_id does not exist in the details - اراضى تكميلى
            det_lst.insert(1, None)     # Insert empty item for Land_ID

        rec_lst += det_lst
    else:
        rec_lst = rec_lst + ['' for i in range(11)]
        project_type = None
    return rec_lst, project_type


def parse_tech_rec(txt, line_no, out_error, dt, log_type):

    global search_tokens

    # log_type = 'ERROR'
    if len(txt) < 90:   # empty error
        rec_lst = [dt, None, line_no, None, log_type, None, None, None, 'Empty Error', 'tech' , 0, 0, None] 
        return rec_lst

    project_id = 0 
    try:
        task_id = int(txt[99:102])
    except:
        task_id = None
    classified= False
    
    for token in search_tokens.itertuples():
        # if txt.find(token.token) != -1: # found
        if token.token in txt: # found, time optimization
            # print (token, "------->", token) 
            # error_token = token.token
            if pd.isnull(token.desc):
                error_token = token.token
            else:
                error_token = token.desc
            error_categ = token.categ

            txt= None   # error text is not needed in this case
            classified = True
            break    

    if not classified: #categ not found
        out_error.write(f"Unclassified Error. Line no:{line_no}: "+txt)
        error_token = "Unclassified"
        error_categ = 'Unclassified'

    rec_lst = [dt, None, line_no, None, log_type, None, None, None, error_token, error_categ,  project_id, task_id, txt]    

    return rec_lst


# summerize log file(s). save summary to csv file with  
def summerize_portal_logs(fpath, admin=False):
    global Exception_File

    # log_df = log_2_df (fpath)
    if type(fpath) == str:
        exception_file_name = os.path.basename(fpath)+'_exception.log'
    else:
        exception_file_name = fpath.name+'_exception.log'
    Exception_File = os.path.join(SUMMARY_FOLDER, f"{exception_file_name}")
    open(Exception_File,'wt').close()   # clear file
    log_df = zip_log_to_df(fpath)       # summerize one zip file
    # log_df['dt'] = pd.to_datetime(log_df['log_date']).dt.date 

    # x = log_df[['dt', 'token','categ', 'line_no']].fillna('x').groupby(['dt', 'token','categ'],as_index = False).count().\
    #     sort_values(by=['dt', 'categ', 'line_no'], ascending=False)
    # print ('Loading done ...')
    # dts = sorted(log_df.dt.unique())
    # log_df = log_df.assign(log_date= pd.to_datetime(log_df['log_date']))
    # dts = sorted(log_df['log_date'].dt.date.unique())
    # for d in dts: # split multi-date log file into a seperate zip csv file for each day 
    d = log_df['log_date'].dt.date.unique()[0]
    if admin:
        out_file_path = os.path.join(SUMMARY_FOLDER, f"log summary admin-{d}.zip")
    else:
        out_file_path = os.path.join(SUMMARY_FOLDER, f"log summary-{d}.zip")
       
        # df = log_df.loc[log_df.log_date.dt.date == d]
        
    log_df.to_csv(out_file_path, index=False, compression={'method': 'zip', 'archive_name': f"log summary-{d}.csv"})
        # df.to_csv('./out/xx.csv', index=False)
        # update_summary_db(df)

def display_login_cntry(df, selected_dts):
    global Cntry
    
    if df.empty:
        return None
    # df = All_df.copy()
    if len (selected_dts) > 0:  # no selection means select all
        df = df[df.dt.isin(selected_dts)]
    df.rename (columns={'line_no':'Count'}, inplace = True)
    df.country.fillna('not logged', inplace=True)
    # dts_from, dts_to, _ = get_df_dates()

    df = df.loc[df.token =='Logins', ['Count', 'country']]
    df = df.set_index('country').join(Cntry.set_index('Alpha-2 code'), how = 'left').groupby(['Country']).count().sort_values(['Count'], ascending = False)
    # print(df.columns)
    # df = df.index.rename({'dt':'Count'})
    return df.reset_index()
    if not df.empty:
        df_pivot = pd.pivot_table(df, index = 'Country', columns='dt', values = 'Count', aggfunc='count', margins=True, fill_value=0)
        # df_pivot.sort_values(df_pivot.columns[-1], inplace= True, ascending=False)   # sort by totals columns
        # dts_from, dts_to, dts = get_df_dates(df)
        return df_pivot
    else:
        return None

def display_reservation_cntry(df, selected_dts):
    
    if df.empty:
        return None
    # df = All_df.copy()
    if len (selected_dts) > 0:  # no selection means select all
        df = df[df.dt.isin(selected_dts)]
    # df.rename (columns={'line_no':'Count'}, inplace = True)
    df.country.fillna('not logged', inplace=True)
    # dts_from, dts_to, _ = get_df_dates()
   
    df = df.loc[df.token.isin(['Logins', 'confirmLandReservation True']), ['token', 'country', 'line_no', 'NID']]
    df = df.set_index('country').join(Cntry.set_index('Alpha-2 code'), how = 'left')
    if not df.empty:
        df_pivot = pd.pivot_table(df, index = 'Country', columns='token', values = 'line_no', aggfunc='count', margins=False, fill_value=0)
        df_pivot['NID'] = df[['Country', 'NID']].groupby('Country').nunique()
        df_pivot['avg'] = (df_pivot.Logins/df_pivot.NID).astype(int)
        df_pivot = df_pivot.sort_values('Logins', ascending=False).reset_index()
        if len(df_pivot.columns) == 4:
            df_pivot.columns = ['Country', '# Logins', '# Customers', 'Avg']
        else:
            df_pivot.columns = ['Country', '# Logins', '# Reservations', '# Customers', 'Avg']
        return df_pivot


def top_login_customers_during_reservation(df, start_time):

    if df.empty:
        return None
    if start_time:
        df = df[pd.to_datetime(df.log_date) > start_time]
    
    df = df.loc[df.token.isin(['Logins', 'confirmLandReservation True', 'confirmReservation True']),['NID', 'token', 'log_date', 'IP_address']]
    df.log_date = df.log_date.dt.date 
    x = pd.pivot_table(df, index= 'NID', columns = 'token', values='log_date', aggfunc='count').sort_values('Logins', ascending= False)

    if 'confirmLandReservation True' not in x.columns and 'confirmReservation True' not in x.columns:  # None reservation logs
        x['Reservation'] = ''
    # elif 'confirmLandReservation True' in x.columns:        # Land reservation
    #     x['Reservation'] = x['confirmLandReservation True'].fillna('')
    # elif 'confirmReservation True' in x.columns:       # unit reservation
    #     x['Reservation'] = x['confirmReservation True'].fillna('')
    x = x.reset_index()
    
    x.columns = ['NID', '# Logins', 'Reservation']  
    x.loc[x.Reservation == 1, ['Reservation']] = 'True'
    # x['NID'] = x['NID'].astype('int64')#.astype(str)
    x['# Logins'] = x['# Logins'].fillna(0).astype(int)
    no_ips = df[['NID', 'IP_address']].groupby('NID').nunique()
    no_ips.index = no_ips.index.astype(str)
    x.NID = x.NID.astype(str)
    x = pd.concat([x.set_index('NID'),no_ips], axis = 'columns')    # concat side by side using index
    x = x.rename(columns = {'IP_address':'# of IPs'})
    return x.fillna('')

def filter_df(df, selected_dts, selected_tokens):
    if selected_dts and selected_tokens:
        df = df[df.dt.isin(selected_dts) & df.token.isin(selected_tokens)]
    elif not selected_dts:
        df = df[df.token.isin(selected_tokens)]
    elif not selected_tokens:
        df = df[df.dt.isin(selected_dts)]
    else:
        pass# work of all df
    return df

def login_stats(df):
    # st.write(df.columns)
    x = (df.loc[df.token.isin(['Logins', 'confirmLandReservation True', 'confirmReservation True'])]
         .assign(dt=lambda x: x.log_date.dt.date))[['NID', 'token', 'dt']]
    x = pd.pivot_table(x, index= 'NID', columns = 'token', values='dt', aggfunc='count').sort_values('Logins', ascending= False)
    if 'confirmLandReservation True' not in x.columns and 'confirmReservation True' not in x.columns:
         x['confirmLandReservation True'] = 0
    # x['confirmLandReservation True'] = x['confirmLandReservation True'].fillna(0)
    x = x.reset_index()
    x.columns = ['NID', '# Logins', 'Reservation']  
    # x.loc[x.Reservation == 1.0, ['Reservation']] = 'True'
    x['# Logins'] = x['# Logins'].fillna(0).astype(int)

    bins = [0, 10, 50, 100, 9999]   # distplay stats in bins
    labels = ['1 to 10', '11 to 50', '51 to 100', '> 100']
    x['bins'] = pd.cut(x['# Logins'], bins = bins, labels= labels)
    # x.to_csv('./out/xxx.csv', index=False)
    stats = x[['bins', '# Logins', 'Reservation']].groupby('bins').sum()
    # print (stats.columns)
    stats['NID'] = x.drop_duplicates('NID').groupby(['bins']).count()['NID']
    stats['avg'] = (stats['# Logins']/stats['NID']).fillna(0).astype(int)
    stats = stats.fillna(0).astype(int).reset_index()
    # stats.columns = [' No of logins per customer', '# of customers', 'Total # logins', '# of reservations',  'Avg logins/ customer']
    # print (stats)
    # stats = stats.iloc[:,[3,0,1,2, 4]] # relocate

    return stats

def plot_log_summary(df, selected_dts, selected_tokens):

    dfx = filter_df(df, selected_dts, selected_tokens)

    # dfx = dfx[['dt', 'token', 'line_no']][dfx.categ == 'user'].groupby(['dt','token']).count().sort_values('line_no', ascending=False)
    dfx = dfx[['dt', 'token', 'line_no']].groupby(['dt','token']).count().sort_values('line_no', ascending=False)
    dfx = dfx.rename(columns={'line_no':'Count'}).reset_index()
    
    
    if len(selected_dts) == 1:      # single day
        fig = dfx.pivot(index='token', columns='dt', values = 'Count').plot.barh(figsize=(10,5)).get_figure()
    else:   # multi day graph
        fig = dfx.pivot(index='dt', columns='token', values = 'Count').plot(kind = 'line', figsize=(10,6), rot=45).get_figure()
    return fig


def get_df_data(df, selected_dts, selected_tokens):

    df = filter_df(df, selected_dts, selected_tokens)
    
    df_pivot = pd.pivot_table(df, index = ['categ','token'], columns='dt', values = 'line_no', aggfunc='count', margins=False, fill_value=0)
    return df_pivot


def get_tokens(df, categ=None):

    if categ == None:
        return df.token.unique()
    else:   # user categ only 
        return df[df.categ == categ].token.unique()

def get_reservation_nid(df):
    # print (df.NID.dtypes)
    # if df.NID.dtypes == float:
    #     df.NID = df.NID.fillna(0).astype(np.int64)
    # df.NID = df.NID.fillna('').astype(str)
    cust_nos = df.loc[df.token.isin(['confirmLandReservation True', 'confirmReservation True']), ['NID', 'log_date']].sort_values(['log_date', 'NID'])
    
    res_data = df.loc[df.NID.isin(cust_nos.NID), ['NID','log_date', 'token' ]]
    # 24-01-2024  
    # res_data.token.loc[res_data.token == 'confirmLandReservation True'] = 'Land Reservation' 
    res_data.loc[res_data.token == 'confirmLandReservation True', 'token'] = 'Land Reservation'
    
    # cust_list = cust_nos.NID + " --> Reservation time: " + cust_nos.log_date.dt.strptime('%Y-%m-%d %H:%M:%S')
    cust_list = cust_nos.NID + " --> Reservation time: " + cust_nos.log_date.astype(str)

    return cust_list, res_data


def update_summary_db(df):
    df_pivot = pd.pivot_table(df, index = ['categ','token'], columns='dt', values = 'line_no', aggfunc='count', margins=False, fill_value=0)
    return df_pivot
    pass


def load_project_table(uploaded_file):

    df = pd.read_excel(uploaded_file, skiprows=1)
    rep_date = df.iloc[-1]['project_id']
   
    last_rep_date = get_last_proj_rep_date()
    # print('********', rep_date, type(rep_date), last_rep_date, type(last_rep_date))
    if last_rep_date and last_rep_date == str(rep_date):
        # print ("File Already Loaded, ", "****", rep_date, last_rep_date)
        return None # file already loaded
    else:
        df = df[:-1]    # delete last row, it has timestamp of the report
        df[df.columns[8:12]]  = df[df.columns[8:12]].fillna(0).astype(int) # counts are read as float, convert to int
        df.publish_date = df.publish_date.dt.strftime('%Y-%m-%d')
        df.publish_end_date = df.publish_end_date.dt.strftime('%Y-%m-%d')
        df.start_date = df.start_date.dt.strftime('%Y-%m-%d')
        df.end_date = df.end_date.dt.strftime('%Y-%m-%d')
        df = df.drop(columns=['Unnamed: 3', 'Unnamed: 11', 'Unnamed: 13'], axis=1)
        db.save_df(df, tbl_name='project', if_exists='replace')

        rep_date_df = pd.DataFrame(data=[rep_date], columns=['rep_date']).set_index('rep_date')
        # print ("rep_date_df ****:\n", rep_date_df)
        db.df_to_sql(rep_date_df, table = 'project_rep_date', if_exists='append')
        return rep_date
    

def get_last_proj_rep_date():
    sql = "SELECT rep_date FROM project_rep_date ORDER BY rep_date DESC LIMIT 1"
    last_rep_date = db.query_to_list(sql, return_header=False)
    if last_rep_date:
        return last_rep_date[0][0]
    else:
        return None


def quote_log_file(zip_file, option: int, quote:str, from_line:int, to_line: int, no_lines:int):
    # from datetime import datetime
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
    # st.write(datetime.now(), 'before yield_zip_file ....')
    zfiles = yield_zip_file(zip_file, file_type)
    # st.write(datetime.now(),'After yield_zip_file ....')

    for f, file_size in zfiles:
    # f = zfiles.__next__
        # st.write(datetime.now(),'before line count....')
        # line_count = sum(1 for _ in f)
        
        # f.seek(0)

        # st.write(datetime.now(),'Line Count: ', line_count)
        # my_prog_bar = st.progress(0, text="")
        # percent_complete = 0
    # while(True):
        line_no = 1
        lst = []
        line_nos=[]
        max_no_lines=1
        if option == 1:     # quote range of lines
            # skip from_line numbers
            for _ in f: 
                if line_no >= from_line: break
                line_no += 1

            st.write(from_line, to_line, line_no)
            while line_no <= to_line:
                txt = f.readline()
                if not txt: break
                if type(txt) == bytes: txt = txt.decode('utf-8')
                lst.append(txt[:-1])    # exclude \n
                line_nos.append(line_no)
                line_no += 1
        
        elif option ==2:    # quote lines with given string
            while True:
                txt = f.readline().decode('utf-8')
                if not txt: break # end of file
                
                if quote in txt:
                    lst.append(txt)
                    line_nos.append(line_no)

        elif option ==3:    # quote lines  with given string in a range
            # skip from_line lines
            if from_line:
                for _ in f: # skip from line_no 
                    if line_no >= from_line: break
                    line_no += 1

            while True:
                if to_line != 0 and line_no > to_line: break
                txt = f.readline()
                if not txt: break # end of file
                if type(txt) == bytes: txt = txt.decode('utf-8')                
                if not quote or (quote and quote in txt):
                    lst.append(txt)
                    line_nos.append(line_no)
                    max_no_lines +=1
                    if no_lines != 0 and max_no_lines> no_lines: break
                line_no += 1
                # if line_no > 5000: break    # debug
                # if line_no < 100000000: continue
                # if line_no % 10000 == 0: print (line_no)
                # if line_no % 40000 == 0: # and type(zip_file) != str:    # dont show progress for big files
                    #print (line_no)
                    # percent_complete =  int(line_no/line_count*100)
                    # percent_complete =  50
                    # my_prog_bar.progress(percent_complete, text=f"Operation in progress. Please wait- {line_no}/{line_count}" )
        return pd.DataFrame(data=lst, index= line_nos, columns=['log'])