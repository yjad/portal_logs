import streamlit as st
import plogs
import os

# replace_it = st.radio("Log reservation data for project {project_id} already exists, replace it?", options=('Replace','Yes'))
# if replace_it == 'Yes':
#     "db.exec_cmd(f'DELETE FROM {log_table_name} WHERE proj_id = {project_id}'')"
# elif replace_it == 'No':
#     st.write(f"option is {replace_it}")


# st.help(st.radio)
uploaded_files= st.file_uploader('Select Log file',type=["zip", 'gz'], accept_multiple_files = False)
# f= st.file_uploader('Select Log file',type=["log"], accept_multiple_files = False)
# fn = r"C:\Users\yahia\OneDrive - Data and Transaction Services\Python-data\PortalLogs\logs\server.log.2022-04-03.tar.gz"
if st.button("start"):
    zfiles = plogs.yield_zip_file(uploaded_files, '.gz')
    for f in zfiles:

        # with open(fn, 'rt', errors='ignore') as f:
        #     count = sum(1 for _ in f)
        # count = 100000
        # file_size = os.path.getsize(fn)
        f.seek(0,2) # end
        file_size = f.tell()
       
        count = int(file_size/136)   # avrage 136 bytes/line
        st.write(count)
        f.seek(0,0)
        progress_text = "Operation in progress. Please wait- "
        # my_bar = st.progress(0, text=progress_text)
        placeholder = st.empty()
        line_no = 0
        user_count = 0
        while True:
        
            txt = f.readline()
            if not txt:
                break
            if 'WebRequestInterceptor' in txt:
                user_count+=1
            line_no = line_no+1
            if line_no % 10000 == 0: 
                # percent_complete =  int(line_no/count*100)
                # my_bar.progress(percent_complete, text=progress_text+str(line_no))
                # st.text(f"{percent_complete} - {progress_text} {line_no}")
                placeholder.write(f"{line_no}/{count}", disabled=True)
        # my_bar.progress(100, text=progress_text+str(line_no))
        st.write('User count: ', user_count)
        st.write("## Done")
        st.button('reset')