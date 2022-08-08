import json
import os
from numpy import r_

from tzlocal import get_localzone_name

# from numpy import int32
import DB as db 
from datetime import datetime#, timedelta
import pandas as pd
import country_codes
import tokens_data

DATA_FOLDER = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\data" 
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


def log_2_df(file_path):
   
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
            if txt.find('WebRequestInterceptor') != -1: #('"nid"') != -1: #log_type == 'ERROR' and , time optimization
                rec = parse_nid_rec(txt, line_no, out_error, dt)
                # pass
            else:
                rec  = parse_tech_rec(txt, line_no, out_error, dt)

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


def parse_tech_rec(txt, line_no, out_error, dt):

    global search_tokens

    log_type = 'ERROR'
    if len(txt) < 90:   # empty error
        rec_lst = [dt, None, line_no, None, log_type, None, None, None, 'Empty Error', 'tech' , None]
        return rec_lst

    classified= False
    for token in search_tokens.itertuples():
        if txt.find(token.token) != -1: # found
            # print (token, "------->", token) 
            # error_token = token.token
            if pd.isnull(token.desc):
                error_token = token.token
            else:
                error_token = token.desc
            error_categ = token.categ
            classified = True
            break    

    if not classified: #categ not found
        out_error.write(f"Unclassified Error. Line no:{line_no}: "+txt)
        error_token = "Unclassified"
        error_categ = 'Unclassified'

    rec_lst = [dt, None, line_no, None, log_type, None, None, None, error_token, error_categ, txt]

    return rec_lst


# summerize log file(s). Load logins summary into DB and save summary csv file with  
def summerize_portal_logs(fpath, load_db=False):

    log_df = log_2_df (fpath)
    log_df['dt'] = pd.to_datetime(log_df['log_date']).dt.date 

    # x = log_df[['dt', 'token','categ', 'line_no']].fillna('x').groupby(['dt', 'token','categ'],as_index = False).count().\
    #     sort_values(by=['dt', 'categ', 'line_no'], ascending=False)
    print ('Loading done ...')
    dts = sorted(log_df.dt.unique())
    out_file_path = os.path.join(DATA_FOLDER, f"log summary-{dts[0]}-to-{dts[-1]}.csv")
    log_df.to_csv(out_file_path, index=False)
   

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
    print(df.columns)
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
        df_pivot = df_pivot.sort_values('Logins', ascending=False).reset_index()
        df_pivot.columns = ['Country', '# Logins', '# Reservations', '# Customers']
        return df_pivot


def top_login_customers_during_reservation(df, start_time):

    # start_time = '2022-08-07 09:00'

    if df.empty:
        return None
    # if len (selected_dts) > 0:  # no selection means select all
        # df = df[df.dt.isin(selected_dts)]
    if start_time:
        df = df[pd.to_datetime(df.log_date) > start_time]
    
    df = df.loc[df.token.isin(['Logins']), ['line_no', 'NID']] # successful logins
    df = df.groupby('NID').count().sort_values('line_no', ascending=False)#[:50]
    # if not df.empty:
    #     df_pivot = pd.pivot_table(df, index = 'Country', columns='token', values = 'line_no', aggfunc='count', margins=False, fill_value=0)
    #     df_pivot['NID'] = df[['Country', 'NID']].groupby('Country').nunique()
    #     df_pivot = df_pivot.sort_values('Logins', ascending=False).reset_index()
    df =df.reset_index()
    df.columns = ['NID', '# successfull Logins']
    return df

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


def plot_log_summary(df, selected_dts, selected_tokens):

    dfx = filter_df(df, selected_dts, selected_tokens)

    dfx = dfx[['dt', 'token', 'line_no']][dfx.categ == 'user'].groupby(['dt','token']).count().sort_values('line_no', ascending=False)
    dfx = dfx.rename(columns={'line_no':'Count'}).reset_index()
    
    
    if len(selected_dts) == 1:      # single day
        fig = dfx.pivot(index='token', columns='dt', values = 'Count').plot(kind='bar').get_figure()
    else:   # multi day graph
        fig = dfx.pivot(index='dt', columns='token', values = 'Count').plot(kind = 'line', figsize=(10,6)).get_figure()
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
