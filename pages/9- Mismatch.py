import streamlit as st
import pandas as pd
import os
import st_utils as stu

@st.cache_data
def load_db_file(db_file_path):
     dbdf= pd.read_csv(db_file_path, low_memory=False, dtype={'applicant_national_id':str})
     return dbdf

# @st.cache_data
# def load_log_file(log_file_path, query):
#     return (pd.read_csv(log_file_path, low_memory=False, dtype={'NID':str})
#             .query(query)
#             .drop(columns=['node', 'task_id','project_id', 'error_line']))
                    
# project_types = {'وحدات سكنية':1,'أراضى':2, 'مشروع مكمل لأراضى':4}
# LOG_SUMMARY_FOLDER = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\summary"
# project_details_fn = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\checksum\Statistics_of_all_projects.xls"
TIBCO_OUT_FOLDER = r"C:\Users\yahia\OneDrive - Data and Transaction Services\DTS-data\PortalLogs\checksum"
# # unit_db_checksum_fn = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\checksum\NewQueryUnit.zip"
# # land_db_checksum_fn = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\checksum\NewQueryLand.zip"
db_checksum_fn = r"C:\Users\yahia\OneDrive - Data and Transaction Services\DTS-data\PortalLogs\checksum\checksum.zip"

logdf, proj_dict, _ = stu.load_log_summary(multi=False)
    
# st.write(proj_dict)

if not logdf.empty :   # a project selected
    project_type = proj_dict['project_type']
    project_id = proj_dict['project_id'][0]

    if project_type == 1:
        query_token = "token == 'confirmReservation True'"  
        db_file_name = db_checksum_fn
    else: 
        query_token = "token == 'confirmLandReservation True'"
        db_file_name = db_checksum_fn
            # b1, b2= st.columns(2)
            # option = st.radio("Options: ", ['NID Log', 'Match checksum',  'Match Details(LE04)'], horizontal=True )
    option = st.radio("Options: ", ['NID Log', 'Match checksum'], horizontal=True )
        # if b1.button('Match cheksum'):
    if option == 'Match checksum':
        with st.spinner("Reading log & DB files, please wait ..."):
            dbdf= load_db_file(db_file_name).query(f"project_id == {project_id}")
            # logdf = load_log_file(log_file_path, query_token)
            logdf = logdf.query(query_token)
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

                mismatch = (mismatch.astype({'land_id': str})
                            .assign(land_id = lambda x: x[unit_land].str[:-2]) # remove ".0" 
                            .rename({'check_sum':'DB checksum', 'checksum': 'Log checksum', 'land_id':unit_land}, axis=1))
                st.dataframe(mismatch[['NID', unit_land, 'DB checksum', 'Log checksum']])
                

        if project_type == 1:   # Units
            tibco_fn = os.path.join(TIBCO_OUT_FOLDER, f'UE04_{project_id}.xls')
            rename_dict = {
                'رقم الوحدة': 'Unit_No',
                'رقم العمارة': 'building_no',
                'النموذج': 'Unit_Model',
                'الرقم القومى':'NID'}
            col_list = [0,2,3,18]
        else:
            tibco_fn = os.path.join(TIBCO_OUT_FOLDER, f'LE04_{project_id}.xls')
            rename_dict = {'نسبة التميز': 'excellence_ratio',
                'مساحة الأرض':'land_size',
                'رقم الأرض':'Land_No',
                'كود المجاورة':'Sub_District',
                'الرقم القومى':'NID'}
            col_list = [0,1,2,17]
        if not os.path.exists(tibco_fn):
            st.error(f"TIBCO file name for this project does not exist: {tibco_fn}")
        else:
            le04 = (pd.read_excel(tibco_fn, skiprows=1, dtype={'الرقم القومى':str},)
                    .iloc[:,col_list].rename(rename_dict, axis=1))
            
            # logdf = load_log_file(log_file_path, query_token) 
            def mmatch(x):
                if 'Unit_No_db' in x.index:
                    if x.Unit_No_db != x.Unit_No_log or \
                        x.building_no_db != x.building_no_log or \
                        x.Unit_Model_db != x.Unit_Model_log:
                        return False
                else:
                    try:
                        if x.Land_No_db != x.Land_No_log or \
                            x.land_size_db != x.land_size_log or \
                            x.excellence_ratio_db != x.excellence_ratio_log:
                            return False
                    except:
                        # st.write(x)
                        return (x.Land_No_db == x.Land_No_log)
                            
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
            query_token += f" and NID == '{NID}'" 
            # logdf, proj_dict = stu.load_log_summary(multi=False, query = query_token)
            # st.write(logdf.columns)
            logdf = (logdf.query(query_token)
                        # .drop(columns=['node', 'task_id','project_id', 'error_line'])
                        )
            if logdf.empty:
                st.error("National ID does not exist ...")
            else:
                if project_type == 1:
                    col_list = ['Floor_No', 'building_no', 'Unit_No', 'Unit_ID']
                else:
                    
                    col_list  = ['Land_No', 'land_size', 'Land_ID']
                    
                try:
                    logdf[col_list] = logdf[col_list].astype(int)
                except:
                    col_list  = ['Land_No', 'land_size']
                    logdf[col_list] = logdf[col_list].astype(int)
                st.dataframe(logdf.T, use_container_width=True)
            

