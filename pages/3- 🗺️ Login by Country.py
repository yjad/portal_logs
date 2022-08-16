import streamlit as st
import plogs as logs 
import Home as stu

csv_files= st.file_uploader('Select Log summary file',type=["zip"], accept_multiple_files = True)
df, strt, end, dts = stu.upload_csv_files(csv_files)
if strt: # files selected
    selected_dates = st.multiselect('Dates', dts, dts)
    
    with st.spinner("Please Wait ... "):
        df = logs.display_reservation_cntry(df, selected_dates)
    if not selected_dates:

        txt = (f'#### Log data during {strt} to {end}')
    else:
        txt = f'#### # of Logins by country during {selected_dates[0]} to {selected_dates[-1]}'
    st.markdown (txt)
    if not df.empty:
        st.dataframe(df)
        st.download_button(label = 'Save to csv', data = stu.convert_df(df), file_name = 'Login By Country.csv', mime = 'text/csv')
    else:
        st.info('#### No data to display')