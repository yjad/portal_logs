import streamlit as st
import pandas as pd
import base64
import io
import os

# @st.experimental_memo(suppress_st_warning=True)
def upload_csv_files(csv_files):
    if not csv_files:
        st.warning('No csv files loaded, load files first ...') 
        return None, None, None, None
    else:
        with st.spinner("Please Wait ... "):
            All_df = pd.DataFrame() # reset
            for f in csv_files:
                df_1  = pd.read_csv(f,  compression= 'zip', dtype={'NID':str}, low_memory=False)
                # df_1  = pd.read_csv(f, low_memory=False)
                All_df = pd.concat([All_df, df_1])
            All_df.drop_duplicates(subset = ['dt', 'line_no'], inplace=True)
            dts = sorted(All_df.dt.unique())
            strt = dts[0]
            end =dts[-1]
        
        return All_df, strt, end, dts
    

def convert_df (df):
    return df.to_csv().encode('utf-8')

def display_data_dates(strt, end):
    if strt != end:
        txt = f'Data Loaded from {strt} to {end}'
    else:
        txt = f'Data Loaded for {strt}'
    st.subheader(txt)

def csv_download(df, index, file_name, link_display_title = 'Download CSV File'):
    csv = df.to_csv(index=index)
    b64 = base64.b64encode(csv.encode()).decode()  # strings <-> bytes conversions
    href = f'<a href="data:file/csv;base64,{b64}" download="{file_name}">{link_display_title}</a>'
    return href

def excel_download(df, index, file_name, link_display_title = 'Download excel file'):
    file_name = file_name
    towrite = io.BytesIO()
    df.to_excel(towrite, index=index, header=True, sheet_name = os.path.splitext(file_name)[0][:31])
    towrite.seek(0)  # reset pointer
    b64 = base64.b64encode(towrite.read()).decode()  # some strings
    href= f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{file_name}">{link_display_title}</a>'
    return href

