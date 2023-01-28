
import pandas as pd
import streamlit as st
import plogs as logs 
import Home as stu
import DB as db

csv_files= st.file_uploader('Select Log summary file',type=["zip"], accept_multiple_files = True)
df, dt_from, dt_to, dts = stu.upload_csv_files(csv_files)

if not dt_from: 
    st.info('#### No data to display')
else:
    append_data = False
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