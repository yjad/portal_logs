
import pandas as pd
import streamlit as st
import plogs as logs 
import Home as stu

csv_files= st.file_uploader('Select Log summary file',type=["zip"], accept_multiple_files = True)
df, dt_from, dt_to, dts = stu.upload_csv_files(csv_files)

if not dt_from: 
    st.info('#### No data to display')
else:
    stu.display_data_dates(dt_from, dt_to)
    selected_dates = st.multiselect('Dates', dts, dts)
    tokens = df.token.unique()
    # return All_df[All_df.categ == categ].token.unique()
    selected_tokens = st.multiselect('Error Tokens', tokens)
    with st.spinner("Please Wait ... "):
        log_stats = logs.get_df_data(df, selected_dates, selected_tokens)
    if not selected_dates:
        st.markdown (f'#### Log data during {dt_from} to {dt_to}')
    else:
        st.markdown (f'#### Log data during {selected_dates[0]} to {selected_dates[-1]}')
    st.dataframe(log_stats)
    col1, col2 = st.columns(2)
    col1.download_button(label = 'Save to csv', data = stu.convert_df(log_stats), file_name = 'Log Summary Data.csv', mime = 'text/csv')
    res_data = df.loc[df.service.isin( ('confirmReservation', 'confirmLandReservation'))]
    col2.download_button(label = 'Res. Data', data = stu.convert_df(res_data), file_name = f'Res. Data_{selected_dates[0]}.csv', mime = 'text/csv')
