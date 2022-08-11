import pandas as pd
import streamlit as st
import plogs as logs 
import st_utils as stu

csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
df, dt_from, dt_to, dts = stu.upload_csv_files(csv_files)

if dt_from:  
    stu.display_data_dates(dt_from, dt_to)
    cust_nos, cust_journy = logs.get_reservation_nid(df)
    cust_no = st.selectbox("Select NID?", cust_nos)
    st.dataframe(cust_journy.loc[cust_journy.NID == cust_no])
    st.download_button(label = 'Save to csv', data = stu.convert_df(cust_journy), file_name = 'Reservation customer journy.csv', mime = 'text/csv')
