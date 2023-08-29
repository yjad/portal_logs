
import pandas as pd
import streamlit as st
import plogs as logs 
import st_utils as stu
import DB as db
from os import path

def load_summary_file(accept_multiple = False):
    csv_files= st.file_uploader('Select Log summary file',type=["zip"], accept_multiple_files = accept_multiple)
    return csv_files


def log_summary_all():
    st.subheader("Compare logs")
    opt = st.checkbox("Reservation Project?", value=False) 
    if opt== False:
        # csv_files= st.file_uploader('Select Log summary file',type=["zip"], accept_multiple_files = True)
        csv_files = load_summary_file(accept_multiple = True)
        if csv_files:
            df, dt_from, dt_to, dts = stu.upload_csv_files(csv_files)
            proj_dict = {'project_id':None, 'project_type':None, 'start_date': dt_from, 'end_date': dt_to }
        else:
            return
    elif opt == True:
        df, proj_dict, dts = stu.load_log_summary(True)
        
    if df.empty: 
        st.info('#### No data to display')
        return
    
    append_data = False
    stu.display_data_dates(dts[0], dts[-1])
    selected_dates = st.multiselect('Dates', dts, dts)
    tokens = df.token.unique()
    # return All_df[All_df.categ == categ].token.unique()
    selected_tokens = st.multiselect('Error Tokens', tokens)
    with st.spinner("Please Wait ... "):
        log_stats = logs.get_df_data(df, selected_dates, selected_tokens)
    if not selected_dates:
        st.markdown (f'#### Log data during {dts[0]} to {dts[-1]}')
    else:
        st.markdown (f'#### Log data during {selected_dates[0]} to {selected_dates[-1]}')
    st.dataframe(log_stats)
    
    # col1, col2 = st.columns(2)
    if st.checkbox('Save ResData'):
        res_data = df.loc[df.service.isin( ('confirmReservation', 'confirmLandReservation'))]
        if res_data.size > 0:   # are There reservations data in the log file? ...
            proj_date = df.dt.unique()
            if len (proj_date) > 1:
                st.warning ("There are more than dt in the log file")
                proj_date
            else:
                proj_date = proj_date[0]
                project = db.query_to_pd(f"SELECT project_id, project_type_id FROM project WHERE start_date = '{proj_date}'")#, return_header=False)
                if len(project) == 0:
                    st.warning (f"No projects in the database with the reseravtion date: {proj_date}")
                elif len(project) > 1: # there were more than one reservation project in the log file date
                    st.warning (f"reservation of more than project in the same day, log_res_xxxx.proj_id needs to be set manually ...")
                    project_id = 9999   # to be changed manually
                    project_type_id = project.project_type_id.unique() # assuming one type, land or unit
                    # check if records already loaded for the same project
                    if project_type_id in (1,3):
                        table_name = 'res_unit'  
                        log_table_name = 'log_res_unit'
                    else: #'res_land'   
                        table_name = 'res_land'  
                        log_table_name = 'log_res_land'
                    append_data = True
                else: #one project in the log file 
                    project_id = project.loc[0,'project_id']
                    project_type_id = project.loc[0,'project_type_id']
                    if project_type_id in (1,3):
                        table_name = 'res_unit'  
                        log_table_name = 'log_res_unit'
                    else: #'res_land'   
                        table_name = 'res_land'  
                        log_table_name = 'log_res_land'
                    append_data =True
    if append_data:
        sql = f"SELECT count() FROM {log_table_name} WHERE proj_id = {project_id}"
        count = db.query_to_list(sql, return_header=False)
        if count:  # log data already loaded
            option = st.radio(f"Log reservation data for project {project_id} already exists, replace it?", options=('No Action','No','Yes'))
            if option == 'Yes':
                db.exec_cmd(cursor= None, cmd= f"DELETE FROM {log_table_name} WHERE proj_id = {project_id}")
                f"DELETE FROM {log_table_name} WHERE proj_id = {project_id}"
                save = True
            else:
                save = False
        else:
            save = True
        if save:  
            res_data['proj_id'] = project_id        # log res data does not have project-id
            db.df_to_sql(res_data, log_table_name, if_exists="append")
            st.info("Done ...")
        # st.download_button(label = 'Res. Data', data = stu.convert_df(res_data), file_name = f'Res. Data_{selected_dates[0]}.csv', mime = 'text/csv')


def res_rate():
    # csv_files =load_summary_file(True)
    # if not csv_files:
    #     return
    
    # # df = pd.read_csv(csv_files, compression='zip', dtype_backend='pyarrow', parse_dates=['log_date'])
    # df, _,_, _= stu.upload_csv_files(csv_files)
    # if df.size == 0: return
    df, _, dts = stu.load_log_summary(True)
    
    if df.empty: 
        st.info('#### No data to display')
        return
    
    df = df.assign (log_date = pd.to_datetime(df.log_date))
    # st.write(df.log_date.dtype)
    df = (df
            .assign (hr = df.log_date.dt.hour)
            .assign(min = df.log_date.dt.minute)
            .assign(date = df.log_date.dt.date))

    # log_file_date = path.split(csv_files[0].name)[1][12:22]
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
            plot(title=f"{project_type} Reservation rate by Hour/Min {dts}", rot=45,figsize=(10,5)).get_figure()
        st.pyplot(fig)
    else:
        st.info("## No reservation on that log ...")


def res_by_gov():
    csv_files =load_summary_file(True)
    if not csv_files:
        return
    df, _,_, _= stu.upload_csv_files(csv_files)
    nid_gov = pd.read_csv(r".\portal_gov.csv")[['nid_gov_code', 'portal_gov_name']].drop_duplicates(subset='portal_gov_name')
    land_proj= df.query("token == 'confirmLandReservation True'").count()[0]
    if land_proj > 0:
        st.title("Land Reservation By Governrate ")
    else:
        st.title("Unit Reservation By Governrate ")

    
    dfo= (df.query("token in ('confirmLandReservation True', 'confirmReservation True')")[['NID', 'City']]
        .assign(nid_gov_code= lambda x: x.NID.str[7:9].astype(int))
        .merge(nid_gov, how='left', on= 'nid_gov_code')
        .reset_index(drop=True)
        # .drop(['nid_gov_code', 'gov_code', 'city_code', 'city_name'], axis=1)
        .drop(['nid_gov_code'], axis=1)
        .rename(columns={'NID': '# of Reservations',
                'portal_gov_name':'birth_gov',
                'City':'Reservaion City'})
        .groupby([  'birth_gov','Reservaion City',]).count()
        .sort_values(by= '# of Reservations', ascending=False)
    )

    st.dataframe(dfo)

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

options={   '...':None, 
            '1- Log Summary all': log_summary_all,
            # '2- Tech log stats ': tech_log, 
            # '3- Summarize log exception file': summarize_log_exceptions_file,
            # "4- Load Portal Projects' Data": load_db_project_table 
            "3- Reservation Rate": res_rate, 
            "4- Reservation by birth Governrate": res_by_gov,
        }

opt = st.sidebar.selectbox("Options",options.keys())
if options[opt]: 
    options[opt]() # execute option
