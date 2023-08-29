import os
import pandas as pd
import streamlit as st
import plogs as logs 
import st_utils as stu
import DB as db

def logins_by_country():
    df, proj_dict, _ = stu.load_log_summary(False)
    if not df.empty:
        # selected_dates = st.multiselect('Dates', dts, dts)
        selected_dates=[]
        with st.spinner("Please Wait ... "):
            df = logs.display_reservation_cntry(df, selected_dates)
        # if not selected_dates:

        txt = (f"#### Log data during {proj_dict['start_date']}")
        # else:
        #     txt = f'#### # of Logins by country during {selected_dates[0]} to {selected_dates[-1]}'
        st.markdown (txt)
    if not df.empty:
        st.dataframe(df)
        st.download_button(label = 'Save to csv', data = stu.convert_df(df), file_name = 'Login By Country.csv', mime = 'text/csv')
    else:
        st.info('#### No data to display') 

def no_of_logins():
    df, proj_dict, _ = stu.load_log_summary(False)

    if not df.empty:
        st.write("### No logins stats")
        df2 = logs.login_stats(df)
        st.dataframe(df2)
        st.download_button(label = 'Save to csv', data = stu.convert_df(df2), file_name = 'Reservation Logins Stats.csv', mime = 'text/csv')

        st.write("### No logins Details")
        # stu.display_data_dates(strt, end)
        times = sorted(list(pd.to_datetime(df.log_date.unique())))
        
        start_time = st.select_slider('Start time?', options = times)

        df1 = logs.top_login_customers_during_reservation(df, start_time)
        
        no_of_recs = st.select_slider('No of customers?', options = range(1, len(df)), value = 10)
        st.dataframe(df1[:no_of_recs])
        # df['NID'] = df['NID'].astype(str)   # convert long NID to string
        # df['# Logins'] = df['# Logins'].astype(int)   # convert long NID to string
        st.download_button(label = 'Save to csv', data = stu.convert_df(df1[:no_of_recs]), file_name = 'Top customers # of logins.csv', mime = 'text/csv')


def res_rate():
    df, proj_dict, _ = stu.load_log_summary(False)
    if df.empty:
        return
    
    df = (df
            .assign (hr = df.log_date.dt.hour)
            .assign(min = df.log_date.dt.minute)
            .assign(date = df.log_date.dt.date))

    # log_file_date = os.path.split(csv_files[0].name)[1][12:22]
    log_file_date = proj_dict['start_date']
    if df.query("token == 'confirmLandReservation True'").shape[0] != 0: 
        token = 'confirmLandReservation True'
        project_type = 'Land'
    elif df.query("token == 'confirmReservation True'").shape[0] != 0: 
        token = 'confirmReservation True'
        project_type = 'Unit'
    else: 
        token=None
        project_type = None
    
    df = df.rename({'log_date': '# of reservations'}, axis=1)
    if token:
        fig = df.query(f"token == '{token}'")[['# of reservations', 'hr', 'min',]].groupby(['hr', 'min']).count().\
            plot(title=f"{project_type} Reservation rate by Hour/Min {log_file_date}", rot=45,figsize=(10,5)).get_figure()
        st.pyplot(fig)
    else:
        st.info("## No reservation on that log ...")


def res_by_gov():
    df, proj_dict, _ = stu.load_log_summary(False)
    if df.empty:
        return
    nid_gov = pd.read_csv(r".\portal_gov.csv")[['nid_gov_code', 'portal_gov_name']].drop_duplicates(subset='portal_gov_name')
    land_proj= df.query("token == 'confirmLandReservation True'").count()[0]
    if land_proj > 0:
        st.write("### Land Reservation By Governrate ")
    else:
        st.write("### Unit Reservation By Governrate ")

    
    # dfo= (df.query("token in ('confirmLandReservation True', 'confirmReservation True')")[['NID', 'City']]
    #     .assign(nid_gov_code= lambda x: x.NID.str[7:9].astype(int))
    #     .merge(nid_gov, how='left', on= 'nid_gov_code')
    #     .reset_index(drop=True)
    #     # .drop(['nid_gov_code', 'gov_code', 'city_code', 'city_name'], axis=1)
    #     .drop(['nid_gov_code'], axis=1)
    #     .rename(columns={'NID': '# of Reservations',
    #             'portal_gov_name':'birth_gov',
    #             'City':'Reservaion City'})
    #     .groupby([  'birth_gov','Reservaion City',]).count()
    #     .sort_values(by= '# of Reservations', ascending=False)
    # )

    # st.dataframe(dfo)

    dfoo= (df.query("token in ('confirmLandReservation True', 'confirmReservation True')")[['NID', 'City']]
    .assign(nid_gov_code= lambda x: x.NID.str[7:9].astype(int))
    .merge(nid_gov, how='left', on= 'nid_gov_code')
    .reset_index(drop=True)
    # .drop(['nid_gov_code', 'gov_code', 'city_code', 'city_name'], axis=1)
    .drop(['nid_gov_code'], axis=1)
    .groupby(['City', 'portal_gov_name',]).count()
    .reset_index()
)
    total_res_city = dfoo.drop(columns='portal_gov_name').groupby(['City']).sum()
    dfoo = (dfoo.merge(total_res_city, how='left', on='City')
    .assign(percent= lambda x: (100*x['NID_x']/x['NID_y']).astype(int))
    .rename(columns={'NID_x': '# of Reservations',
                'City':'Reservaion City',
                'portal_gov_name':'birth_gov'})
    .groupby(['Reservaion City', 'birth_gov']).sum()
    .drop(columns=['NID_y'])
    .sort_values(by=['Reservaion City', 'percent'], ascending=[True, False])
)
    st.dataframe(dfoo)

def log_details_by_NID():
    df, _, _ = stu.load_log_summary(False)
    if df.shape[0] != 0 :
        search_by =st.radio("Search By", ["National ID", "IP Address"], horizontal=True)
        NID = st.text_input('xxx', placeholder=search_by, label_visibility='hidden')
        if st.button('List') and df.shape[0] != 0 and len(NID) != 0:
            if search_by ==  "National ID":
                col = 'NID'
            else:
                col = 'IP_address'
        
            q= f"{col} == '{NID}'"

            NID_df = df.query(q)[['log_date', 'NID', 'IP_address', 'token']]
            st.dataframe(NID_df)
            st.download_button(label = 'Save to csv', data = stu.convert_df(NID_df), file_name = f'log_data_NID_{NID}.csv', mime = 'text/csv')

def log_DB_logins():
    st.subheader('# of Logins/# of Paid Customers')
    log_df, proj_dict, _ = stu.load_log_summary(False) # 
    if log_df.shape[0] != 0 :
        
        df = log_df.token.value_counts()
        no_logins = df.loc['Logins']
        proj_id = proj_dict['project_id'][0]
        df = db.query_to_pd(f"select * from project where project_id = {proj_id}")

        
        # st.write(pd.DataFrame(proj_dict))
        no_paid_customers = df.iloc[0,10]
        ratios = {'proj_id':proj_id, 'no_logins':no_logins, 
                   'no_paid_customers':no_paid_customers, 
                   'ratio': int(no_logins/no_paid_customers)}
       
        df = pd.DataFrame(ratios,  index=[0]).set_index('proj_id')
        df
        
def analyze_tech_log():
    # csv_files= st.file_uploader('Select Log summary file',type=["zip"], accept_multiple_files = True)
    # csv_files =load_summary_file(True)
    # if not csv_files:
    #     return
    
    # df, _,_, _= stu.upload_csv_files(csv_files)
    # df = pd.read_csv(csv_files, compression='zip', low_memory=False)
    if st.checkbox("Reservation Log?",value=True):
        df, _, _ = stu.load_log_summary(True)
        
        if df.empty: 
            st.info('#### No data to display')
            return
    else:
        log_file_path = st.file_uploader("Select log file:", ["zip"], accept_multiple_files=False)
        if log_file_path:
            df = stu.load_log_file(log_file_path)
        else:
            return
    df.error_line.fillna(' ', inplace = True)
    # df1 = df.loc[df.categ == 'tech'].pivot_table(index= ['service', 'error_line'], values = 'line_no', columns = 'token', aggfunc='count', fill_value=0)#.to_csv('./out/xxx.csv', index = True)
    # df1 = df.loc[df.categ == 'tech']
    df1 = pd.pivot_table(df, index = ['categ','token'], columns='dt', values = 'line_no', aggfunc='count', margins=False, fill_value=0)

    # df1 = df[df.categ == 'tech'][['service',  'token']].groupby('token').aggregate('count').reset_index()
    # df1.rename ({'service':'Count'}, axis=1, inplace=True)
    df1 = df1.assign(Select = False)
    need_details = st.data_editor(df1.reset_index())#, hide_index=True)
    opt= need_details.loc[need_details["Select"]==True]['token']
    if not opt.empty:
        df2 = df.loc[df.token == opt.iloc[0], ['service', 'log_date', 'error_line']].\
            groupby(['service', 'error_line']).\
            aggregate('count').sort_values('log_date', ascending=False).\
            assign (get_details = False)
        # st.dataframe(df2.reset_index())
        edited_df = st.data_editor(df2.reset_index())
        service = edited_df.loc[edited_df["get_details"]==True]['service']
        if not service.empty:
            st.dataframe(df.loc[df.service == service.iloc[0]])

options={   '...':None, 
            '0- Analyze log file': analyze_tech_log,
            '1- Logins by country': logins_by_country,
            '2- # of logins ': no_of_logins, 
            "3- Reservation Rate": res_rate, 
            "4- Reservation by birth Governrate": res_by_gov,
            "5- Search Log by NID": log_details_by_NID,
            "6- # of Logins/# of Paid Customers": log_DB_logins,
        }

opt = st.sidebar.selectbox("Options",options.keys())
if options[opt]: 
    options[opt]() # execute option