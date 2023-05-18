import pandas as pd
import streamlit as st
import plogs as logs 
import Home as stu

# csv_files= st.file_uploader('Select Log summary file',type=["zip"], accept_multiple_files = True)
# df, dt_from, dt_to, dts = stu.upload_csv_files(csv_files)
df, proj_dict = stu.load_log_summary(False)

if not df.empty:  

    # stu.display_data_dates(dt_from, dt_to)
    cust_nos, cust_journy = logs.get_reservation_nid(df)
    # st.write(cust_journy)
    cust_no = st.selectbox("Select NID?", cust_nos)
    if cust_no:
        cust_no = cust_no[:14]
        st.dataframe(cust_journy.loc[cust_journy.NID == cust_no].reset_index(drop=True))
        st.download_button(label = 'Save to csv', data = stu.convert_df(cust_journy), file_name = 'Reservation customer journy.csv', mime = 'text/csv')
