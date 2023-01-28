import streamlit as st

replace_it = st.radio("Log reservation data for project {project_id} already exists, replace it?", options=('Replace','Yes'))
if replace_it == 'Yes':
    "db.exec_cmd(f'DELETE FROM {log_table_name} WHERE proj_id = {project_id}'')"
elif replace_it == 'No':
    st.write(f"option is {replace_it}")


st.help(st.radio)

