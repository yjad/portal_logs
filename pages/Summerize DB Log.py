import streamlit as st
import plogs as logs 
import pandas as pd
from os import path

DATA_FOLDER = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\DB-data" 

def read_db_log(uploaded_files):

    file_type = path.splitext(uploaded_files.name)[1]
    print (file_type)
    zfiles = logs.yield_zip_file(uploaded_files, file_type)
    all_data = []
    for f in zfiles:
        line_no = 0
        # print (fname)
        while True:
            rec = f.readline()
            # print (len(txt), txt)
            if not rec: break # end of file
            line_no += 1
            if line_no % 10000 == 0: print (line_no)
            # if line_no > 1000: break

            # for rec in f:
            rec = rec.decode()
            lst = rec.split()
           
            if len(lst) >=4:
                rec_lst = [lst[i] for i in range(4)]    # take first 4 values seprated by \t
                x = rec.find(rec_lst[3])                # take the sql command 
                rec_lst[3] = rec_lst[3].upper()         # unify the command 
                # print (len(lst), x)
                rec_lst.append(rec[x:])
            else:
                rec_lst = [x for x in lst]
            all_data.append(rec_lst)

    df = pd.DataFrame(data=all_data, columns=['timestamp', 'ID', 'xxxx', 'type', 'query'])

    return df


# uploaded_file= st.file_uploader('Select Log file',type=["zip", 'gz', 'log'], accept_multiple_files = False)
uploaded_file= st.file_uploader('Select Log file',type=['zip','log'], accept_multiple_files = False)
if st.button('Process ...') and uploaded_file:
    with st.spinner("Please Wait ... "):
        df = read_db_log(uploaded_file)
        dts = df.timestamp.str[:10].unique()
        for d in dts: # split multi-date log file into a seperate zip csv file for each day 
            if d and d[:4].isnumeric():
                out_file_path = path.join(DATA_FOLDER, f"log summary-{d}.zip")
                df.loc[df.timestamp.str[:10] == d].to_csv(out_file_path, index=False, compression={'method': 'zip', 'archive_name': f"DBlog summary-{d}.csv"})
            
        # st.dataframe(dts)
    st.success("File extraced ...")


