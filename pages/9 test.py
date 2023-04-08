import streamlit as st

# replace_it = st.radio("Log reservation data for project {project_id} already exists, replace it?", options=('Replace','Yes'))
# if replace_it == 'Yes':
#     "db.exec_cmd(f'DELETE FROM {log_table_name} WHERE proj_id = {project_id}'')"
# elif replace_it == 'No':
#     st.write(f"option is {replace_it}")


st.help(st.download_button)

# fn = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\logs\server.log.2022-04-03.tar.gz"
# if st.button("start"):


#     with open(fn, 'rt', errors='ignore') as f:
#         count = sum(1 for _ in f)

#     progress_text = "Operation in progress. Please wait- "
#     my_bar = st.progress(0, text=progress_text)

#     with open(fn, 'rt', errors='ignore') as f:
#         line_no = 0
#         percent_complete = 0
#         while True:
           
#             txt = f.readline()
#             if not txt:
#                 break
#             line_no = line_no+1
#             if line_no % 1000 == 0: 
#                 percent_complete =  int(line_no/count*100)
#                 my_bar.progress(percent_complete, text=progress_text+str(line_no))
#         my_bar.progress(100, text=progress_text+str(line_no))
#     st.write("## Done")
#     st.button('reset')