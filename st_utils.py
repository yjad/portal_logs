import streamlit as st
import pandas as pd

@st.experimental_memo(suppress_st_warning=True)
def upload_csv_files(csv_files):
    if not csv_files:
        st.warning('No csv files loaded, load files first ...') 
        return None, None, None, None
    else:
        with st.spinner("Please Wait ... "):
            All_df = pd.DataFrame() # reset
            for f in csv_files:
                df_1  = pd.read_csv(f, low_memory=False)
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
