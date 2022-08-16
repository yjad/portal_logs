import pandas as pd
import streamlit as st
import plogs as logs 
import Home as stu

csv_files= st.file_uploader('Select Log summary file',type=["zip"], accept_multiple_files = True)
df, strt, end, dts = stu.upload_csv_files(csv_files)
if strt: # files selected
        stu.display_data_dates(strt, end)
        times = sorted(list(pd.to_datetime(df.log_date.unique())))
        
        start_time = st.select_slider('Start time?', options = times)

        df = logs.top_login_customers_during_reservation(df, start_time)
        no_of_recs = st.select_slider('No of customers?', options = range(1, len(df)), value = 10)
        st.dataframe(df[:no_of_recs])
        # df['NID'] = df['NID'].astype(str)   # convert long NID to string
        # df['# Logins'] = df['# Logins'].astype(int)   # convert long NID to string
        st.download_button(label = 'Save to csv', data = stu.convert_df(df[:no_of_recs]), file_name = 'Top customers # of logins.csv', mime = 'text/csv')
