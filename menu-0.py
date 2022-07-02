# from base64 import decode
# from numpy import true_divide
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import plogs as logs 
import matplotlib.pyplot as plt


st.title('Reservation Portal Logs - followup')


# def plot_failed_logins(df):
#     x = df[['token', 'dt', 'line_no']].loc[df.token.isin(['Logins', 'Failed Logins'])].groupby(['dt','token']).count().reset_index()
#     fig = x.pivot(index='dt', columns='token', values = 'line_no').plot().get_figure()
    
#     return st.pyplot(fig)

with st.sidebar:
    selected = option_menu('Main Menu',["Home", "Load Log Files", 'Load log summary csv', 'Reports', 'Settings'], 
        icons=['house', '', '', '', 'gear'], menu_icon="cast", default_index=0)      #, default_index=1
    # selected

rep_option = ''
match selected:
    case "Load Log Files":
        uploaded_files= st.file_uploader('Select Log file',type=["txt","log"], accept_multiple_files = True)
        if st.button('Process ...'):
            pd = logs.load_portal_logs(uploaded_files, load_db=True)
            st.dataframe(pd)
    case 'Load log summary csv':
        uploaded_csv_files= st.file_uploader('Select Log summary file',type=["csv"], accept_multiple_files = False)
        if st.button('Process ...'):
                csv_log_df = pd.read_csv(uploaded_csv_files)
                st.write(f'file {uploaded_csv_files} loaded, len = {csv_log_df.count()} ')
                
    case 'Reports':
        rep_option = st.sidebar.selectbox('Reports',
        ('Email Quota Error', 'Failed Logins'))

st.write(f'**********Outside match file  loaded, len = {csv_log_df.count()} ')
match rep_option:
    case 'Email Quota Error':
        fig = logs.plot_email_quota_error()
        st.pyplot(fig)
    
    case 'Failed Logins':
        st.write(f'XXXXXXXXXXXXXXXXXXXXXX  loaded, len = {csv_log_df.count()} ')
        # st.write(csv_log_df)
        # logs.plot_failed_logins(csv_log_df)
        # try:
        #     logs.plot_failed_logins(csv_log_df)
        # except :
        #     st.error ('Load the log file summary first...')

# st.markdown("""
# This app retrieves the list of the **S&P 500** (from Wikipedia) and its corresponding **stock closing price** (year-to-date)!
# * **Python libraries:** base64, pandas, streamlit, numpy, matplotlib, seaborn
# * **Data source:** [Wikipedia](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies).
# """)

# st.sidebar.header('User Input Features')




# if st.button('Show Plots'):
#     st.header('Email Quota Limit grph')
#     plot_1()

# if st.button('Dataframe'):
#     # st.header('Email Quota Limit grph')
#     df = logs.export_email_quota_graph()
#     df.iloc[:,2] = df.iloc[:,2].fillna(0).astype('int32')
#     st.dataframe(df)

def x():
    df = logs.export_email_quota_graph().fillna(0)
    sorted_date_unique = sorted( df['dt'].unique() )
    selected_dt = st.sidebar.multiselect('Log Date', sorted_date_unique, sorted_date_unique)

    df_selected_sector = df[ (df['dt'].isin(selected_dt)) ]
    st.header('Display log categs in Selected Date(s)')
    st.write('Data Dimension: ' + str(df_selected_sector.shape[0]) + ' rows and ' + str(df_selected_sector.shape[1]) + ' columns.')
    st.dataframe(df_selected_sector)


