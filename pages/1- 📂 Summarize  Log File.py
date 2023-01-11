import streamlit as st
import plogs as logs 



def summarize_log_file():
    uploaded_files= st.file_uploader('Select Log file',type=["zip", 'gz'], accept_multiple_files = False)
    if st.button('Process ...') and uploaded_files:
        with st.spinner("Please Wait ... "):
            logs.summerize_portal_logs(uploaded_files, load_db=False)
    st.success("File extraced ...")

def summarize_log_exceptions_file():
    uploaded_files= st.file_uploader('Select Log file',type=["log"], accept_multiple_files = False)
    if st.button('Process ...') and uploaded_files:
        with st.spinner("Please Wait ... "):
            logs.summerize_exception_file(uploaded_files)
    st.success("File extraced ...")


options={'...':None, 
            'Summarize log file': summarize_log_file,
            'Summarize log exception file': summarize_log_exceptions_file,
        }

opt = st.sidebar.selectbox("Options",options.keys())
if options[opt]: 
    options[opt]() # execute option