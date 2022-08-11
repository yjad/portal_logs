# from base64 import decode
# from numpy import true_divide
# from distutils.util import strtobool
from ast import Pass
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import plogs as logs 
import matplotlib.pyplot as plt

UPLOADED_CSV_FILE = None
# st.title('Reservation Portal Logs - followup')


# def plot_failed_logins(df):
#     x = df[['token', 'dt', 'line_no']].loc[df.token.isin(['Logins', 'Failed Logins'])].groupby(['dt','token']).count().reset_index()
#     fig = x.pivot(index='dt', columns='token', values = 'line_no').plot().get_figure()
    
#     return st.pyplot(fig)

# @st.cache(suppress_st_warning=True)
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
        txt = f'#### Data Loaded for the duration from {strt} to {end}'
        st.success(txt)
        return All_df, strt, end, dts
        
        

def convert_df (df):
    return df.to_csv().encode('utf-8')

with st.sidebar:
    selected = option_menu('Portal Logs',
    ["Home", 'Log to csv', 
            'Big Log>200MB to csv', 
            # 'Load log summary',
            # 'Email Quota grpah',
            # 'Failed Logins', 
            "Reservation Logins Stats",
            'Login/Reservation by country',
            "Customers' # of logins",
            'Log summary, Data',
            'Log summary, Plot',
            "Reservation's customer journy",
            'Settings'], 
        icons=['house', '', '', '', '','', '', '', 'gear'], menu_icon="cast", default_index=0)      #, default_index=1
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

    # case 'Load log summary':
    #     csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
    #     # print (uploaded_csv_file)
    #     # if st.button('Process ...'):
    #     if not csv_files:
    #         st.warning('No csv files loaded, load files first ...') 
    #     else:
    #         with st.spinner("Please Wait ... "):
    #             # strt, end, _ = logs.combine_log_csv(uploaded_csv_file)
    #             strt, end, _ = upload_csv_files(csv_files)
            
    #         txt = f'#### Data Loaded for the duration from {strt} to {end}'
    #         st.success(txt)
    #         # st.markdown (txt)

    case 'Email Quota grpah':
        fig = logs.plot_email_quota_error()
        st.pyplot(fig)


    case 'Login/Reservation by country':
        csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
        df, strt, end, dts = upload_csv_files(csv_files)
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
                st.download_button(label = 'Save to csv', data = convert_df(df), file_name = 'Login By Country.csv', mime = 'text/csv')
            else:
                st.info('#### No data to display')
    
    case "Reservation Logins Stats":
        csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
        df, strt, end, dts = upload_csv_files(csv_files)
        if strt: # files selected
            df = logs.login_stats(df)
            st.dataframe(df)
            st.download_button(label = 'Save to csv', data = convert_df(df), file_name = 'Reservation Logins Stats.csv', mime = 'text/csv')

    case "Customers' # of logins":
        csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
        df, strt, end, dts = upload_csv_files(csv_files)
        if strt: # files selected
            times = sorted(list(pd.to_datetime(df.log_date.unique())))
            
            start_time = st.select_slider('Start time?', options = times)

            df = logs.top_login_customers_during_reservation(df, start_time)
            no_of_recs = st.select_slider('No of customers?', options = range(1, len(df)), value = 10)
            st.dataframe(df[:no_of_recs])
            df['NID'] = df['NID'].astype(str)   # convert long NID to string
            st.download_button(label = 'Save to csv', data = convert_df(df[:no_of_recs]), file_name = 'Top customers # of logins.csv', mime = 'text/csv')


    case 'Log summary, Data':
        csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
        df, dt_from, dt_to, dts =upload_csv_files(csv_files)
        
        if not dt_from: 
            st.info('#### No data to display')
        else:
            selected_dates = st.multiselect('Dates', dts, dts)
            tokens = df.token.unique()
            # return All_df[All_df.categ == categ].token.unique()
            selected_tokens = st.multiselect('Error Tokens', tokens, tokens)
            with st.spinner("Please Wait ... "):
                df = logs.get_df_data(df, selected_dates, selected_tokens)
            if not selected_dates:
                st.markdown (f'#### Log data during {dt_from} to {dt_to}')
            else:
                st.markdown (f'#### Log data during {selected_dates[0]} to {selected_dates[-1]}')
            st.dataframe(df)
            st.download_button(label = 'Save to csv', data = convert_df(df), file_name = 'Log Summary Data.csv', mime = 'text/csv')

                

    case 'Log summary, Plot':

        csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
        df, dt_from, dt_to, dts = upload_csv_files(csv_files)

        if not dt_from:  
            st.info ("No data to plot")
        else:
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

    case "Reservation's customer journy":
        csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = True)
        df, dt_from, dt_to, dts = upload_csv_files(csv_files)

        if dt_from:  
            cust_nos, cust_journy = logs.get_reservation_nid(df)
            cust_no = st.selectbox("Select NID?", cust_nos)
            st.dataframe(cust_journy.loc[cust_journy.NID == cust_no])
            st.download_button(label = 'Save to csv', data = convert_df(cust_journy), file_name = 'Reservation customer journy.csv', mime = 'text/csv')

                            

def x():
    df = logs.export_email_quota_graph().fillna(0)
    sorted_date_unique = sorted( df['dt'].unique() )
    selected_dt = st.sidebar.multiselect('Log Date', sorted_date_unique, sorted_date_unique)

    df_selected_sector = df[ (df['dt'].isin(selected_dt)) ]
    st.header('Display log categs in Selected Date(s)')
    st.write('Data Dimension: ' + str(df_selected_sector.shape[0]) + ' rows and ' + str(df_selected_sector.shape[1]) + ' columns.')
    st.dataframe(df_selected_sector)



           