import streamlit as st
import plogs as logs 

filename = st.text_input(label = "Enter file path:")
if st.button('Process ...'):
    with st.spinner("Please Wait ... "):
        logs.summerize_portal_logs(filename, load_db=False)
    
    st.success("File extraced ...")