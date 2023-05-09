import streamlit as st
import pandas as pd
import os

@st.cache_data
def load_db_file(db_file_path):
     dbdf= pd.read_csv(db_file_path, low_memory=False, dtype={'applicant_national_id':str})
     return dbdf

@st.cache_data
def load_log_file(log_file_path, query):
    return (pd.read_csv(log_file_path, low_memory=False, dtype={'NID':str})
            .query(query)
            .drop(columns=['node', 'task_id','project_id', 'error_line']))
                    
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
            # b1, b2= st.columns(2)
            option = st.radio("Options: ", ["Match checksum", 'NID Log', 'Match Details(LE04)'], horizontal=True )
            # if b1.button('Match cheksum'):
            if option == 'Match checksum':
                with st.spinner("Reading log & DB files, please wait ..."):
                    dbdf= load_db_file(db_file_name).query(f"project_id == {project_id}")
                    logdf = load_log_file(log_file_path, query_token)
                            
                    df = (dbdf.merge(logdf, left_on='applicant_national_id', right_on='NID')
                            .assign(match = lambda x:(x.checksum == x.check_sum))
                        )
                    st.write("### Checksum match count")
                    if project_type == 1:
                        st.write("Units count - DB: ", dbdf.shape[0])
                    else:
                        st.write("Lands count - DB: ", dbdf.shape[0])
                    st.dataframe(df.match.value_counts())
                    mismatch = df.query("match == False")
                    if mismatch.empty:
                        pass
                    else:
                        # st.write("### Checksum mismatch details")
                        if project_type == 1:
                            unit_land = 'unit_id'
                        else:
                            unit_land = 'land_id'

                        mismatch = mismatch.astype({'land_id': str}).rename({'check_sum':'DB checksum', 'checksum': 'Log checksum', 'land_id':unit_land}, axis=1)
                        st.dataframe(mismatch[['NID', unit_land, 'DB checksum', 'Log checksum']])
                      

                if project_type == 1:   # Units
                    tibco_fn = os.path.join(r"C:\Users\yahia\Downloads", f'UE04_{project_id}.xls')
                    rename_dict = {
                        'رقم الوحدة': 'Unit_No',
                        'رقم العمارة': 'building_no',
                        'النموذج': 'Unit_Model',
                        'الرقم القومى':'NID'}
                    col_list = [0,2,3,18]
                else:
                    tibco_fn = os.path.join(r"C:\Users\yahia\Downloads", f'LE04_{project_id}.xls')
                    rename_dict = {'نسبة التميز': 'excellence_ratio',
                        'مساحة الأرض':'land_size',
                        'رقم الأرض':'Land_No',
                        'كود المجاورة':'Sub_District',
                        'الرقم القومى':'NID'}
                    col_list = [0,1,2,17]
                if not os.path.exists(tibco_fn):
                    st.error(f"TIBCO file name for this project does not exist: {tibco_fn}")
                else:
                    le04 = (pd.read_excel(tibco_fn, skiprows=1, dtype={'الرقم القومى':str})
                            .iloc[:,col_list].rename(rename_dict, axis=1))
                    
                    # logdf = load_log_file(log_file_path, query_token) 
                    def mmatch(x):
                        if 'Unit_No_db' in x.index:
                            if x.Unit_No_db != x.Unit_No_log or \
                                x.building_no_db != x.building_no_log or \
                                x.Unit_Model_db != x.Unit_Model_log:
                                return False
                        else:
                            if x.Land_No_db != x.Land_No_log or \
                                x.land_size_db != x.land_size_log or \
                                x.excellence_ratio_db != x.excellence_ratio_log:
                                return False
                        return True
                            
                    # le04.columns
                    # logdf.columns
                    df = (le04.merge(logdf, left_on='NID', right_on='NID', suffixes=('_db', '_log'))
                            .assign(match = lambda x: x.apply(mmatch, axis=1))
                        )
                    # df.columns
                    # st.write(df.query("NID == '26411200102736'").T)
                    st.write("### Details match count")
                    if project_type == 1:
                        st.write("Units count - DB: ", le04.shape[0], 'Log: ', logdf.shape[0])
                        col_list = ['NID',  'City','Region', 'Unit_No_log', 'building_no_log', 'Unit_No_db', 'building_no_db']
                    else:
                        st.write("Lands count - DB: ", le04.shape[0], 'Log: ', logdf.shape[0])
                        col_list = ['NID', 'City','Region', 'Land_No_log', 'land_size_log', 'Land_No_db', 'land_size_db',  ]
                    st.dataframe(df.match.value_counts())
                    mismatch = df.query("match == False")[col_list].reset_index(drop=True)
                    if mismatch.empty:
                        pass
                    else:
                        st.dataframe(mismatch, use_container_width=True)

            elif option == 'NID Log':
                NID = st.text_input("NID: ")
                if NID:
                    # query_token += f" and NID == '{NID}'" 
                    logdf = (load_log_file(log_file_path, query_token)
                                .query(query_token)
                                # .drop(columns=['node', 'task_id','project_id', 'error_line'])
                                )
                    if logdf.empty:
                        st.error("National ID does not exisit ...")
                    else:
                        st.dataframe(logdf.T, use_container_width=True)
                
