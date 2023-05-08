import streamlit as st
import pandas as pd
import os

def load_db_file(db_file_path):
     dbdf= pd.read_csv(db_file_path, low_memory=False, dtype={'applicant_national_id':str})
     return dbdf


def load_log_file(log_file_path):
    return pd.read_csv(log_file_path, low_memory=False, dtype={'NID':str})
    


project_types = {'وحدات سكنية':1,'أراضى':2, 'مشروع مكمل لأراضى':4}
LOG_SUMMARY_FOLDER = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\summary"
project_details_fn = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\checksum\Statistics_of_all_projects.xls"
unit_db_checksum_fn = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\checksum\NewQueryUnit.zip"
land_db_checksum_fn = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\checksum\NewQueryLand.zip"
db_checksum_fn = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\checksum\checksum.zip"

# log_date = st.date_input("Log Date:")
# log_sum_fn = os.path.join(LOG_SUMMARY_FOLDER,f"log_summary-{log_date.strftime('%Y-%m-%d')}.zip")
# st.write(log_sum_fn)
# portal_proj_file = st.file_uploader('Select projects file',type=["xlsx", 'xls'], accept_multiple_files = False)
if not os.path.exists(project_details_fn):
        st.error(f"Porjects' details file not exists: {project_details_fn}")

projdf = (pd.read_excel(project_details_fn, skiprows=1, usecols=[0,1,2,4, 5, 10], index_col=0)
    .assign (start_date= lambda x: x.start_date.dt.date)
    .assign (end_date= lambda x: x.end_date.dt.date)
    .assign(select=False)
    .sort_values(by='start_date', ascending=False)
)
selected = st.experimental_data_editor(projdf, height = 200, use_container_width=True).query("select == True")
if not selected.empty:
    if selected.select.count() > 1:
        st.error("Should select only one row, deselect ...")
    else:
        project_id = selected.index[0]
        project_type = project_types.get(selected.project_type_name_ar.iloc[0])
        start_date = selected.start_date.iloc[0]
        end_date = selected.end_date.iloc[0]
        # st.write(project_id, project_type, start_date, end_date)
        if start_date != end_date:  # multi-day project
            lst = pd.date_range(start_date, end_date).date
            log_date = st.selectbox("Select log date", lst)
        else:
            log_date = start_date

        log_file_path = os.path.join(LOG_SUMMARY_FOLDER, f"log summary-{log_date.strftime('%Y-%m-%d')}.zip")
        # st.write(log_file_path)
        if not os.path.exists(log_file_path):
            st.error(f"log file not exists: {log_file_path}")
        else:
            if project_type == 1:
                query_token = "token == 'confirmReservation True'"  
                db_file_name = db_checksum_fn
            else: 
                query_token = "token == 'confirmLandReservation True'"
                db_file_name = db_checksum_fn
            if st.button('Match cheksum'):
                with st.spinner("Reading log & DB files, please wait ..."):
                    dbdf= load_db_file(db_file_name). query(f"project_id == {project_id}")
                    logdf = (load_log_file(log_file_path)
                            .query(query_token)
                            .drop(columns=['node', 'task_id','project_id', 'error_line']))
                    
                    df = (dbdf.merge(logdf, left_on='applicant_national_id', right_on='NID')
                            .assign(match = lambda x:(x.checksum == x.check_sum))
                        )
                    st.write("### Project match count")
                    if project_type == 1:
                        st.write("Units count - DB: ", dbdf.shape[0])
                    else:
                        st.write("Lands count - DB: ", dbdf.shape[0])
                    st.dataframe(df.match.value_counts())
                    mismatch = df.query("match == False")
                    if mismatch.empty:
                        pass
                    else:
                        st.write("### Checksum mismatch details")
                        if project_type == 1:
                            unit_land = 'unit_id'
                        else:
                            unit_land = 'land_id'

                        mismatch = mismatch.astype({'land_id': str}).rename({'check_sum':'DB checksum', 'checksum': 'Log checksum', 'land_id':unit_land}, axis=1)
                        st.dataframe(mismatch[['NID', unit_land, 'DB checksum', 'Log checksum']])
                      
                    # dfx = (logdf.merge(dbdf, left_on='NID', right_on='applicant_national_id')
                    #         .dropna(subset='applicant_national_id')
                    #     )
                    # st.write("No of reservation rows in logs but not in DB:", dfx.shape[0])
                    # st.dataframe(dfx)
