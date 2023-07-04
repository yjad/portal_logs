import pandas as pd
import streamlit as st
import plogs as logs 
import Home as stu




def summarize_log_file():
    uploaded_files= st.file_uploader('Select Log file',type=["zip", 'gz', '7z', 'bz2'], accept_multiple_files = False)
    if st.button('Process ...') and uploaded_files:
        with st.spinner("Please Wait ... "):
            logs.summerize_portal_logs(uploaded_files, load_db=False)
        st.success("File extraced ...")

def Summarize_big_file():
    filename = st.text_input(label = "Enter file path:")
    if st.button('Process ...'):
        with st.spinner("Please Wait ... "):
            logs.summerize_portal_logs(filename, load_db=False)
        st.success("File extraced ...")


def summarize_log_exceptions_file():
    uploaded_files= st.file_uploader('Select Log file',type=["log"], accept_multiple_files = False)
    if st.button('Process ...') and uploaded_files:
        with st.spinner("Please Wait ... "):
            logs.summerize_exception_file(uploaded_files)
    st.success("File extraced ...")

def load_db_project_table():
    uploaded_file= st.file_uploader('Select project file',type=["xlsx", 'xls'], accept_multiple_files = False)
    if st.button('Process ...') and uploaded_file:
        with st.spinner("Please Wait ... "):
            logs.load_project_table(uploaded_file)
    st.success("File Loaded ...")

def quote_log_file():
    file_size_option = st.radio("File Size: ",["< 200 MB", "Big file"], horizontal = True)
    if file_size_option == "< 200 MB":
        uploaded_file= st.file_uploader('Select Log file',type=["zip", 'gz', '7z'], accept_multiple_files = False)
        # uploaded_file_type = 'UPLOAD'
    else:
        uploaded_file = st.text_input(label = "Enter file path:")
        # uploaded_file_type = str

    # option = st.radio("Optios: ",["Quote range", "Quote string"] , horizontal = True)
    # if option == "Quote range":
    #     from_line_no = st.number_input('From Line_no: ', step=1)
    #     to_line_no   = st.number_input('to   Line_no: ', step=1)
    #     quote = None
    #     ioption = 1
    #     out_file_name = f"Log file range{from_line_no}-{to_line_no}.csv"
    # elif option == "Quote string":
    #     quote = st.text_input('Quote to search: ')
    #     from_line_no = to_line_no= None
    #     ioption = 2
    #     out_file_name = f"Log file quote- {quote}.csv"

    # option = st.radio("Optios: ",["Quote range", "Quote string"] , horizontal = True)
    # if option == "Quote range":
    col = st.columns(3)
    from_line_no = col[0].number_input('From Line-no: ', step=1)
    to_line_no   = col[1].number_input('To Line-no: ', step=1)
    no_lines   = col[2].number_input('Max No. of Lines: ', step=1)
    quote = st.text_input('Quote to search (case sensitive): ')
        
    out_file_name = f"Log file q-{quote} ft-{from_line_no}-{to_line_no}.csv"

    if st.button('Process ...') and uploaded_file:
        with st.spinner("Please Wait ... "):
            # st.write(f"calling: quote = {quote}, from_line = {from_line_no}, to_line = {to_line_no}")
            df = logs.quote_log_file(uploaded_file, option = 3, quote = quote, 
                                     from_line = from_line_no, 
                                     to_line = to_line_no,
                                     no_lines= no_lines)
            if df.size == 0: 
                st.warning("No data found ...")
                return 
            st.write ("No of lines: ", len(df))
            st.dataframe (df[:20], use_container_width=True)
            df = df.to_csv().encode('utf-8')
        
        st.download_button(label = 'Save Quote', data =df, file_name = out_file_name, mime = 'text/csv')
    # elif option == "Quote string":
    #     quote = st.text_input('Quote to search: ')
    #     if st.button('Process ...') and uploaded_file:
    #         with st.spinner("Please Wait ... "):
    #             lst = logs.quote_log_file(uploaded_file, option = 2, quote = quote, from_line = None, to_line = None)
            
    #         st.write ("No of lines: ", len(lst))
    #         st.write (lst[:20])
    #         # st.download_button(label = 'Save Quote', data = lst, file_name = 'Login By Country.txt', mime = 'text/csv')

def load_log_summary(multi = False):
    LOG_SUMMARY_FOLDER = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\summary"
    # PROJECT_DETAILS_FN = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\checksum\Statistics_of_all_projects.xls"
    PROJECT_TYPES = {'وحدات سكنية':1,'أراضى':2, 'مشروع مكمل لأراضى':4}

    # st.info(f"Loading projects ....")

    # if not os.path.exists(PROJECT_DETAILS_FN):
    #     st.error(f"Porjects' details file not exists: {PROJECT_DETAILS_FN}")
    #     return pd.DataFrame(), {}
    
    # projdf = (pd.read_excel(PROJECT_DETAILS_FN, skiprows=1, usecols=[0,1,2,4, 5, 10], index_col=0)
    #     .assign (start_date= lambda x: x.start_date.dt.date)
    #     .assign (end_date= lambda x: x.end_date.dt.date)
    #     .assign(select=False)
    #     .rename(columns={'project_type_name_ar':'proj_type', 'No. of reservations':'# Res.'})
    #     .sort_values(by='start_date', ascending=False)
    # )
    projdf = db.query_to_pd("project")
    projdf['select'] = False
    # col_list = projdf.columns[:-1].insert(0,'select')   # insert new colmmn for selection
    # projdf = projdf[col_list]

    selected = st.data_editor(projdf, height = 200, use_container_width=True).query("select == True")
    if selected.select.count() == 0:    # no selelction
        return pd.DataFrame(), None   # empty
    if selected.select.count() != 1:
        st.error("Should select only one row, deselect ...")
        return pd.DataFrame(), None   # empty
    
    project_id = selected.index[0]
    project_type = PROJECT_TYPES.get(selected.proj_type.iloc[0])
    start_date = selected.start_date.iloc[0]
    end_date = selected.end_date.iloc[0]
    if start_date != end_date:  # multi-day project
        lst = pd.date_range(start_date, end_date).date
        log_date = st.selectbox("Select log date", lst)
    else:
        log_date = start_date

    log_file_path = os.path.join(LOG_SUMMARY_FOLDER, f"log summary-{log_date.strftime('%Y-%m-%d')}.zip")
    if not os.path.exists(log_file_path):
        st.error(f"log file not exists: {log_file_path}")
        return pd.DataFrame(), None   # empty
    
    logdf = load_log_file(log_file_path)
    proj_dict = {'project_id':project_id, 'project_type':project_type, 'start_date': start_date, 'end_date': end_date }
    return logdf, proj_dict


options={   '...':None, 
            '1- Summarize log file': summarize_log_file,
            '2- Summarize big-size log file (> 200GB)': Summarize_big_file,
            '3- Summarize log exception file': summarize_log_exceptions_file,
            "4- Load Portal Projects' Data": load_db_project_table, 
            "5- Quote log file": quote_log_file 
        }

opt = st.sidebar.selectbox("Options",options.keys())
if options[opt]: 
    options[opt]() # execute option
