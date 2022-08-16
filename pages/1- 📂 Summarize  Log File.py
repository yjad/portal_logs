import streamlit as st
import plogs as logs 

uploaded_files= st.file_uploader('Select Log file',type=["zip"], accept_multiple_files = False)
if st.button('Process ...') and uploaded_files:
    with st.spinner("Please Wait ... "):
        logs.summerize_portal_logs(uploaded_files, load_db=False)

    st.success("File extraced ...")