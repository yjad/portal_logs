# from base64 import decode
# from numpy import true_divide
# from distutils.util import strtobool
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import plogs as logs 
import matplotlib.pyplot as plt

UPLOADED_CSV_FILE = None
st.title('Reservation Portal Logs - followup')


# def plot_failed_logins(df):
#     x = df[['token', 'dt', 'line_no']].loc[df.token.isin(['Logins', 'Failed Logins'])].groupby(['dt','token']).count().reset_index()
#     fig = x.pivot(index='dt', columns='token', values = 'line_no').plot().get_figure()
    
#     return st.pyplot(fig)

# @st.cache
# def upload_csv_files(files):
#     return logs.combine_log_csv(files)

def convert_df (df):
    return df.to_csv().encode('utf-8')

with st.sidebar:
    selected = option_menu('Main Menu',
    ["Home", 'Log to csv', 
            'Big Log>200MB to csv', 
            'Load log summary',
            # 'Email Quota grpah',
            # 'Failed Logins', 
            'Login countries',
            'Show summary data',
            'Plot log summary',
            'Settings'], 
        icons=['house', '', '', '', '', '','', 'gear'], menu_icon="cast", default_index=0)      #, default_index=1
    # selected

match selected:
    case "Log to csv":
        uploaded_files= st.file_uploader('Select Log file',type=["log"], accept_multiple_files = True)
        if st.button('Process ...'):
            with st.spinner("Please Wait ... "):
                pd = logs.summerize_portal_logs(uploaded_files, load_db=False)

            st.success("File extraced ...")

    case 'Big Log>200MB to csv':
        filename = st.text_input(label = "Enter file path:")
        if st.button('Process ...'):
            with st.spinner("Please Wait ... "):
                logs.summerize_portal_logs(filename, load_db=False)
            
            st.success("File extraced ...")

    case 'Load log summary':
        uploaded_csv_file= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
        # print (uploaded_csv_file)
        # if st.button('Process ...'):
        if not uploaded_csv_file:
            st.warning('No csv files loaded, load files first ...') 
        else:
            with st.spinner("Please Wait ... "):
                strt, end, _ = logs.combine_log_csv(uploaded_csv_file)
            
            txt = f'#### Data Loaded for the duration from {strt} to {end}'
            st.success(txt)
            # st.markdown (txt)

    case 'Email Quota grpah':
        fig = logs.plot_email_quota_error()
        st.pyplot(fig)
    
    # case 'Failed Logins':
    #     uploaded_csv_file= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
    #     if uploaded_csv_file and st.button('Process ...'):
    #         with st.spinner("Please Wait ... "):
    #             fig = logs.plot_failed_logins(uploaded_csv_file)
    #         st.pyplot(fig)

    case 'Login countries':
        if not logs.summary_loaded():
            st.warning('No csv files loaded, load files first ...') 
        else:
            dt_from, dt_to, dts = logs.get_df_dates()
            selected_dates = st.multiselect('Dates', dts, dts)
            
            with st.spinner("Please Wait ... "):
                df, strt, end = logs.display_login_cntry(selected_dates)
            txt = f'#### Login by countries during {strt} to {end}'
            st.markdown (txt)
            if not df.empty:
                st.dataframe(df)
                st.download_button(label = 'Save to csv', data = convert_df(df), file_name = 'Login By Country.csv', mime = 'text/csv')
            else:
                st.info('#### No data to display')

    case 'Show summary data':
        if not logs.summary_loaded():
            st.warning('No csv files loaded, load files first ...') 
        else:
            dt_from, dt_to, dts = logs.get_df_dates()
            selected_dates = st.multiselect('Dates', dts, dts)

            tokens = logs.get_tokens()
            selected_tokens = st.multiselect('Error Tokens', tokens, tokens)

            with st.spinner("Please Wait ... "):
                # df=  upload_csv_files(uploaded_csv_file)
                df, dt_from, dt_to, _ = logs.get_df_data(selected_dates, selected_tokens)
           
            if len(df) > 0:
                st.markdown (f'#### Log data during {dt_from} to {dt_to}')
                st.dataframe(df)
                st.download_button(label = 'Save to csv', data = convert_df(df), file_name = 'Log Summary Data.csv', mime = 'text/csv')
            else:
                st.info('#### No data to display')

    case 'Plot log summary':
        # if not uploaded_csv_file:
        # uploaded_csv_file= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
        
        # if st.button('Process ...'):
        if not logs.summary_loaded():
            st.warning('No csv files loaded, load files first ...') 
        else:
            dt_from, dt_to, dts = logs.get_df_dates()
            selected_dates = st.multiselect('Dates', dts, dts)

            tokens = logs.get_tokens()
            selected_categs = st.multiselect('Error Tokens', tokens, tokens)
            with st.spinner("Please Wait ... "):
                # df=  upload_csv_files(uploaded_csv_file)
                fig, strt, end = logs.plot_log_summary(selected_dates, selected_categs)
                
            txt = f'#### Log Summary during {strt} to {end}'
            st.markdown (txt)
            if fig:
                st.pyplot(fig)
            else:
                st.info ("No data to plot")
                        



def x():
    df = logs.export_email_quota_graph().fillna(0)
    sorted_date_unique = sorted( df['dt'].unique() )
    selected_dt = st.sidebar.multiselect('Log Date', sorted_date_unique, sorted_date_unique)

    df_selected_sector = df[ (df['dt'].isin(selected_dt)) ]
    st.header('Display log categs in Selected Date(s)')
    st.write('Data Dimension: ' + str(df_selected_sector.shape[0]) + ' rows and ' + str(df_selected_sector.shape[1]) + ' columns.')
    st.dataframe(df_selected_sector)



           