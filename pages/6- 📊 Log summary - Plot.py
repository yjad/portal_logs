import pandas as pd
import streamlit as st
import plogs as logs 
import Home as stu

csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
df, dt_from, dt_to, dts = stu.upload_csv_files(csv_files)

if not dt_from:  
    st.info ("No data to plot")
else:
    stu.display_data_dates(dt_from, dt_to)
    selected_dates = st.multiselect('Dates', dts, dts)
    tokens = logs.get_tokens(df, categ = 'user')
    selected_categs = st.multiselect('Error Tokens', tokens, tokens)
    with st.spinner("Please Wait ... "):
        fig = logs.plot_log_summary(df, selected_dates, selected_categs)
    if not selected_dates:
        txt = f'#### Log Summary during {dt_from} to {dt_to}'
    else:
        txt = f'#### Log Summary during {selected_dates[0]} to {selected_dates[-1]}'
    st.markdown (txt)

    if fig:
        st.pyplot(fig)
    else:
        st.info ("No data to plot")