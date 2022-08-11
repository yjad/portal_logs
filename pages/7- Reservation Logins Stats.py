import streamlit as st
import plogs as logs 
import st_utils as stu

csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
df, strt, end, dts = stu.upload_csv_files(csv_files)
if strt: # files selected
    stu.display_data_dates(strt, end)
    df = logs.login_stats(df)
    st.dataframe(df)
    st.download_button(label = 'Save to csv', data = stu.convert_df(df), file_name = 'Reservation Logins Stats.csv', mime = 'text/csv')