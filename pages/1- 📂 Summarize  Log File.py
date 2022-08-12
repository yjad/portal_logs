import streamlit as st
import plogs as logs 

uploaded_files= st.file_uploader('Select Log file',type=["log"], accept_multiple_files = True)
if st.button('Process ...'):
    with st.spinner("Please Wait ... "):
        pd = logs.summerize_portal_logs(uploaded_files, load_db=False)

    st.success("File extraced ...")