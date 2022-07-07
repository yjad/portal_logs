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

with st.sidebar:
    selected = option_menu('Main Menu',
    ["Home", 'Log to csv', 
            'Big Log>200MB to csv', 
            'Load log summary',
            'Email Quota grpah',
            'Failed Logins', 
            'Login countries',
            'Show summary data',
            'Display log summary',
            'Settings'], 
        icons=['house', '', '', '', '', '', '', '','', 'gear'], menu_icon="cast", default_index=0)      #, default_index=1
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
        if st.button('Process ...'):
            with st.spinner("Please Wait ... "):
                logs.combine_log_csv(uploaded_csv_file)

    case 'Email Quota grpah':
        fig = logs.plot_email_quota_error()
        st.pyplot(fig)
    
    case 'Failed Logins':
        uploaded_csv_file= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
        if uploaded_csv_file and st.button('Process ...'):
            with st.spinner("Please Wait ... "):
                fig = logs.plot_failed_logins(uploaded_csv_file)
            st.pyplot(fig)

    case 'Login countries':
        uploaded_csv_file= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
        # if uploaded_csv_file and st.button('Process ...'):
        with st.spinner("Please Wait ... "):
            # df, strt, end = logs.display_login_cntry(uploaded_csv_file)
            df, strt, end = logs.display_login_cntry()
        txt = f'#### Login by countries during {strt} to {end}'
        st.markdown (txt)
        st.dataframe(df)
        df.to_csv (f'.\\out\\{strt}-{end}.csv', index=False)

    # case 'Show summary data':
        
    #     # uploaded_csv_file= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
    #     # if uploaded_csv_file and st.button('Process ...'):
    #     with st.spinner("Please Wait ... "):
    #         # df=  upload_csv_files(uploaded_csv_file)
    #     st.markdown ("#### Show summary data")
    #     # df = pd.read_csv(uploaded_csv_file, low_memory=False)
    #     st.dataframe(df[:50])

    case 'Display log summary':
        # if not uploaded_csv_file:
        # uploaded_csv_file= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
        
        if st.button('Process ...'):
            with st.spinner("Please Wait ... "):
                # df=  upload_csv_files(uploaded_csv_file)
                dts = logs.get_log_dates()
                selected_dts = st.sidebar.multiselect('Dates', dts, dts)
                fig, strt, end = logs.display_log_summary(selected_dts)
            txt = f'#### Log Summary during {strt} to {end}'
            st.markdown (txt)
            st.pyplot(fig)
           

    # case 'Load log summary csv':
    #     uploaded_csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = False)
    #     if st.button('Process ...'):
    #             csv_log_df = pd.read_csv(uploaded_csv_files)
    #             st.write(f'file {uploaded_csv_files} loaded, len = {csv_log_df.count()} ')
                


def x():
    df = logs.export_email_quota_graph().fillna(0)
    sorted_date_unique = sorted( df['dt'].unique() )
    selected_dt = st.sidebar.multiselect('Log Date', sorted_date_unique, sorted_date_unique)

    df_selected_sector = df[ (df['dt'].isin(selected_dt)) ]
    st.header('Display log categs in Selected Date(s)')
    st.write('Data Dimension: ' + str(df_selected_sector.shape[0]) + ' rows and ' + str(df_selected_sector.shape[1]) + ' columns.')
    st.dataframe(df_selected_sector)



           