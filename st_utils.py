import streamlit as st
import pandas as pd
import base64
import io
import os
import DB as db

LOG_SUMMARY_FOLDER = r"C:\Users\yahia\OneDrive - Data and Transaction Services\DTS-data\PortalLogs\summary"
PROJECT_DETAILS_FN = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\summary\Statistics_of_all_projects.xlsx"
PROJECT_TYPES = {'وحدات سكنية':1,'أراضى':2, 'مشروع مكمل لأراضى':4}

# @st.experimental_memo(suppress_st_warning=True)
# @st.cache_data
def upload_csv_files(csv_files):
    if not csv_files:
        st.warning('No csv files loaded, load files first ...') 
        return None, None, None, None
    else:
        with st.spinner("Please Wait ... "):
            All_df = pd.DataFrame() # reset
            for f in csv_files:
                # df_1  = pd.read_csv(f, low_memory=False)
                # df_1  = pd.read_csv(f,  compression= 'zip', dtype={'NID':str}, low_memory=False)
                # 23-04-2023 --> migrate to pandas 2.0 and use pyarrow
                df_1  = pd.read_csv(f,  compression= 'zip', dtype={'NID':'string[pyarrow]'}, engine= 'pyarrow', dtype_backend = 'pyarrow')
                # df_1  = pd.read_csv(f,  compression= 'zip', engine= 'pyarrow', dtype_backend = 'pyarrow')
                # st.write(df_1.NID[:10])
                # print (df_1.info())
                df_1 = (df_1
                        .assign(dt = df_1.log_date.dt.date)
                        .assign(NID = df_1.NID.str[:14]))   # remove '.0'

                All_df = pd.concat([All_df, df_1])
            # All_df.drop_duplicates(subset = ['dt', 'line_no'], inplace=True)
            dts = sorted(All_df.log_date.dt.date.unique())
            strt = dts[0]
            end =dts[-1]
        
        return All_df, strt, end, dts
    

def convert_df (df):
    return df.to_csv().encode('utf-8')

def display_data_dates(strt, end):
    if strt != end:
        txt = f'Data Loaded from {strt} to {end}'
    else:
        txt = f'Data Loaded for {strt}'
    st.subheader(txt)

def csv_download(df, index, file_name, link_display_title = 'Download CSV File'):
    csv = df.to_csv(index=index)
    b64 = base64.b64encode(csv.encode()).decode()  # strings <-> bytes conversions
    href = f'<a href="data:file/csv;base64,{b64}" download="{file_name}">{link_display_title}</a>'
    return href

def excel_download(df, index, file_name, link_display_title = 'Download excel file'):
    file_name = file_name
    towrite = io.BytesIO()
    df.to_excel(towrite, index=index, header=True, sheet_name = os.path.splitext(file_name)[0][:31])
    towrite.seek(0)  # reset pointer
    b64 = base64.b64encode(towrite.read()).decode()  # some strings
    href= f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}" download="{file_name}">{link_display_title}</a>'
    return href

def text_download(lst, index, file_name, link_display_title = 'Download Text File'):
    b64 = base64.b64encode(lst.encode()).decode()  # strings <-> bytes conversions
    href = f'<a href="data:file/csv;base64,{b64}" download="{file_name}">{link_display_title}</a>'
    return href


# @st.cache_data
def load_log_file(log_file_path):
    return (pd.read_csv(log_file_path,  compression= 'zip', dtype={'NID':'string[pyarrow]'}, engine= 'pyarrow', dtype_backend = 'pyarrow')
    # return (pd.read_csv(log_file_path, low_memory=False, dtype={'NID':str})
            # .query(query)
            .assign(NID = lambda x:x.NID.str[:14])   # remove '.0'
            .assign(dt = lambda x: x.log_date.dt.date)
            # .drop(columns=['node', 'task_id','project_id', 'error_line']))
            .dropna(axis=1, how='all'))  # drop empty columns


def load_log_summary(multi = False):


    # st.info(f"Loading projects ....")

    # if not os.path.exists(PROJECT_DETAILS_FN):
    #     st.error(f"Porjects' details file not exists: {PROJECT_DETAILS_FN}")
    #     return pd.DataFrame(), {}

    projdf = (db.query_to_pd("project")
        .set_index('project_id')
        # .drop(columns=['index','Unnamed: 3','Unnamed: 11','Unnamed: 13'], axis=1)
        # .assign (start_date = lambda x: x.start_date)
        # .assign (end_date= lambda x: x.end_date)
        # .assign (publish_date= lambda x: x.publish_date)
        # .assign (publish_end_date= lambda x: x.publish_end_date)
        .rename(columns={'project_type_name_ar':'proj_type', 'No. of reservations':'n_Resv'})
        .assign(n_Resv = lambda x: x.fillna(0).n_Resv.astype(int))
        .sort_values(by='start_date', ascending=False)
    ) 
    projdf.insert(0,'select', False)
    st.write("Select a project:")
    selected = st.data_editor(projdf, height = 200, use_container_width=True).query("select == True")
    if selected.select.count() == 0:    # no selelction
        return pd.DataFrame(), None, None   # empty
    if selected.select.count() != 1 and not multi:
        st.error("Should select only one row, deselect ...")
        return pd.DataFrame(), None, None   # empty
    
    logdf = pd.DataFrame()
    projects_dict = []
    for i in range(selected.shape[0]): 
        project_id = selected.iloc[i:].index
        project_type_str = selected.iloc[i:]['proj_type'].iloc[0]
        project_type = PROJECT_TYPES.get(project_type_str)
        start_date = selected.iloc[i:].start_date.iloc[0]
        end_date = selected.iloc[i:].end_date.iloc[0]
        if start_date != end_date:  # multi-day project
            lst = pd.date_range(start_date, end_date).date
            log_date = st.selectbox("Select log date", lst)
        else:
            log_date = start_date

        log_file_path = os.path.join(LOG_SUMMARY_FOLDER, f"log summary-{log_date}.zip")
        if not os.path.exists(log_file_path):
            st.error(f"log file not exists: {log_file_path}")
            return pd.DataFrame(), None, None   # empty
        
        logdf = pd.concat([logdf, load_log_file(log_file_path)])
        proj_dict = {'project_id':project_id, 'project_type':project_type, 'start_date': start_date, 'end_date': end_date }
        projects_dict.append(proj_dict)

    dts = sorted(logdf.log_date.dt.date.unique())
    # dt_from = dts[0]
    # dt_to =dts[-1]
        
    return logdf, proj_dict, dts


def load_summary_log_date(log_date):
    log_file_path = os.path.join(LOG_SUMMARY_FOLDER, f"log summary-{log_date}.zip")
    if not os.path.exists(log_file_path):
        st.error(f"log file does not exists: {log_file_path}")
        return pd.DataFrame()   # empty
    logdf = load_log_file(log_file_path)

    return logdf
