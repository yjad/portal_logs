import streamlit as st
import plogs as logs 




def summarize_log_file():
    uploaded_files= st.file_uploader('Select Log file',type=["zip", 'gz'], accept_multiple_files = False)
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
    uploaded_file= st.file_uploader('Select project file',type=["csv"], accept_multiple_files = False)
    if st.button('Process ...') and uploaded_file:
        with st.spinner("Please Wait ... "):
            logs.load_project_table(uploaded_file)
    st.success("File Loaded ...")

options={   '...':None, 
            '1- Summarize log file': summarize_log_file,
            '2- Summarize big-size log file (> 200GB)': Summarize_big_file,
            '3- Summarize log exception file': summarize_log_exceptions_file,
            "4- Load Portal Projects' Data": load_db_project_table 
        }

opt = st.sidebar.selectbox("Options",options.keys())
if options[opt]: 
    options[opt]() # execute option
