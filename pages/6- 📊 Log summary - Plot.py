import pandas as pd
import streamlit as st
import plogs as logs 
import st_utils as stu

# csv_files= st.file_uploader('Select Log summary file',type=["zip"], accept_multiple_files = True)
# df, dt_from, dt_to, dts = stu.upload_csv_files(csv_files)


if st.checkbox("Reservation Log?",value=True):
    df, _, dts = stu.load_log_summary(True)
    if df.empty:  
        st.info ("No data to plot")
    else:
        stu.display_data_dates(dts[0], dts[-1])
        selected_dates = st.multiselect('Dates', dts, dts)
        tokens = logs.get_tokens(df, categ = 'user')
        selected_categs = st.multiselect('Error Tokens', tokens)
        with st.spinner("Please Wait ... "):
            fig = logs.plot_log_summary(df, selected_dates, selected_categs)
        if not selected_dates:
            txt = f'#### Log Summary during {dts[0]} to {dts[-1]}'
        else:
            txt = f'#### Log Summary during {selected_dates[0]} to {selected_dates[-1]}'
        st.markdown (txt)
        st.pyplot(fig)
else:
    log_file_path = st.file_uploader("Select log file:", ["zip"], accept_multiple_files=False)
    if log_file_path:
        logdf = stu.load_log_file(log_file_path)
        dts = sorted(logdf.log_date.dt.date.unique())
        tokens = logs.get_tokens(logdf, categ = None)
        selected_categs = st.multiselect('Error Tokens', tokens)
        fig = logs.plot_log_summary(logdf, dts, selected_categs)
    
        st.pyplot(fig)
