import streamlit as st
import plogs as logs 
import pandas as pd
from os import path
import dblogs
import Home

DATA_FOLDER = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\DB-summary" 


def read_db_log(uploaded_files):

    log_pd = dblogs.zip_log_to_df(uploaded_files)
    return log_pd

    # zfiles = logs.yield_zip_file(uploaded_files, file_type)
    # all_data = []
    # for f in zfiles:
    #     line_no = 0
    #     # print (fname)
    #     while True:
    #         rec = f.readline()
    #         # print (len(txt), txt)
    #         if not rec: break # end of file
    #         line_no += 1
    #         if line_no % 10000 == 0: print (line_no)
    #         # if line_no > 1000: break

    #         # for rec in f:
    #         rec = rec.decode()
    #         lst = rec.split()
           
    #         if len(lst) >=4:
    #             rec_lst = [lst[i] for i in range(4)]    # take first 4 values seprated by \t
    #             x = rec.find(rec_lst[3])                # take the sql command 
    #             rec_lst[3] = rec_lst[3].upper()         # unify the command 
    #             # print (len(lst), x)
    #             rec_lst.append(rec[x:])
    #         else:
    #             rec_lst = [x for x in lst]
    #         all_data.append(rec_lst)

    # df = pd.DataFrame(data=all_data, columns=['timestamp', 'ID', 'xxxx', 'type', 'query'])

    # return df

def summarize_db_log():
    uploaded_file= st.file_uploader('Select DB Log file',type=['zip','log', 'rar'], accept_multiple_files = False)
    if st.button('Process ...') and uploaded_file:
        with st.spinner("Please Wait ... "):
            df = dblogs.zip_log_to_df(uploaded_file)
            dts = df.timestamp.dt.date.unique()
            
            for dt in dts: # split multi-date log file into a seperate zip csv file for each day 
                dt_str = str(dt)
                out_file_path = path.join(DATA_FOLDER, f"log summary-{dt_str}.zip")
                df.to_csv(out_file_path, index=False, compression={'method': 'zip', 'archive_name': f"DBlog summary-{dt_str}.csv"})
                st.success(f"File is summarized into '{out_file_path}'")


# @st.experimental_memo (suppress_st_warning=True)
def load_dblog_summary(parse_date=False):
    csv_file= st.file_uploader('Select Summary Log csv file',type=['zip'], accept_multiple_files = False)
    if csv_file:
    # if st.button('Process ...') and csv_file:
        with st.spinner("Please Wait ... "):
            if not csv_file:
                st.warning('No csv files loaded, load files first ...') 
                return None
            else:
                df = pd.read_csv(csv_file, compression= 'zip', parse_dates=parse_date)
                return df
    else:
        return pd.DataFrame()
        
def dblog_by_cmd():
    
    df = load_dblog_summary()
    if df.size != 0: 
        cmds = list(df.cmd.unique())
        cmds.insert(0,'...')
        cmd = st.sidebar.selectbox("Select Command",cmds)
        if cmd != '...':
            dfo = df.loc[df.cmd == cmd]
            if dfo.size == 0:
                st.warning (f'No data for command: {cmd}')
            else:
                dt = dfo.timestamp.iloc[0]
                out_fn= f"DBlog-{cmd}-{dt}.csv"
                st.dataframe(dfo[:100]) # display first 100
                st.markdown(Home.csv_download(dfo, index= True, file_name = out_fn), unsafe_allow_html=True)
            
def dblog_by_query_type():
    
    df = load_dblog_summary()
    if df.size != 0: 
        cmds = list(df.query_type.unique())
        cmds.insert(0,'...')
        cmd = st.sidebar.selectbox("Select Command",cmds)
        if cmd != '...':
            dfo = df.loc[df.query_type == cmd]
            if dfo.size == 0:
                st.warning (f'No data for command: {cmd}')
            else:
                dt = dfo.timestamp.iloc[0]
                out_fn= f"DBlog-{cmd}-{dt}.csv"
                st.dataframe(dfo[:100]) # display first 100
                st.markdown(Home.csv_download(dfo, index= True, file_name = out_fn), unsafe_allow_html=True)
            
    
def dblog_by_proc_tbl():
    
    df = load_dblog_summary()
    if df.size != 0: 
        cmds = list(df.fillna('').proc_tbl.sort_values().unique())
        # cmds = list(df.fillna('').proc_tbl.unique()).sort()
        # list(cmds)#.sort()
        cmds.insert(0,'...')
        cmd = st.sidebar.selectbox("Select Command",cmds)
        if cmd != '...':
            dfo = df.loc[df.proc_tbl == cmd]
            if dfo.size == 0:
                st.warning (f'No data for command: {cmd}')
            else:
                dt = dfo.timestamp.iloc[0]
                out_fn= f"DBlog-{cmd}-{dt}.csv"
                st.dataframe(dfo[:100]) # display first 100
                st.markdown(Home.csv_download(dfo, index= True, file_name = out_fn), unsafe_allow_html=True)
        

def proc_params():
    
    df = load_dblog_summary()
    if df.size != 0: 
        cmds = list(df.fillna('').proc_tbl.sort_values().unique())
        cmds.insert(0,'...')
        cmd = st.sidebar.selectbox("Select Command",cmds)
        if cmd != '...':
            dfo = df.loc[df.proc_tbl == cmd, ['proc_tbl', 'params']]
            nparams = len(dfo.iloc[0,1].split(','))
            params = pd.DataFrame()
            params['proc_tbl'] = dfo.proc_tbl
            t = dfo.params.str.split(',')
            for i in range(nparams) :
                params[f"P{i+1}"] = t.apply(lambda x: x[i].replace("'",""))  
            if params.size == 0:
                st.warning (f'No data for command: {cmd}')
            else:
                dt = df.timestamp.iloc[0]
                out_fn= f"DBlog-{cmd}-{dt}.csv"
                st.dataframe(params) 
                st.markdown(Home.csv_download(params, index= True, file_name = out_fn), unsafe_allow_html=True)


def quot_log_file(csv_fn, from_dttm,to_dttm):
    df1 = load_dblog_summary(parse_date=True)
    
    log_dt = df1.timestamp.iloc[0]
    
    from_time = pd.to_datetime(from_tm)        # ('2022-12-27 10:00')
    to_time= pd.to_datetime(to_tm)             # ('2022-12-27 11:00')
    
    df = df1.loc[pd.to_datetime(df1.timestamp) >= from_time].loc[pd.to_datetime(df1.timestamp) <= to_time]
    
    # csv_fn= os.path.basename(csv_fn).split('.')[0]
    # df.to_csv(f'./out/dblog_from-to_{csv_fn}.csv', index=False )


# 'query_type', 'cmd', 'proc_tbl', 'params'
dblogs_options={'...':None, 
                'Summarize DB log': summarize_db_log,
                'Quot log file from/to': quot_log_file,
                'DBlogs by Query Type': dblog_by_query_type,
                'DBlogs by Command': dblog_by_cmd,
                'DBlogs by procedure/table': dblog_by_proc_tbl,
                'DBlogs by procedure params': proc_params,
                # 'Extarct data from/to',dblog_from_to
                }
opt = st.sidebar.selectbox("Options:",dblogs_options.keys())
if dblogs_options[opt]: 
    dblogs_options[opt]() # execute option
        


