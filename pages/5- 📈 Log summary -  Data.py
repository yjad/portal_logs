
import pandas as pd
import streamlit as st
import plogs as logs 
import Home as stu
import DB as db

csv_files= st.file_uploader('Select Log summary file',type=["zip"], accept_multiple_files = True)
df, dt_from, dt_to, dts = stu.upload_csv_files(csv_files)

if not dt_from: 
    st.info('#### No data to display')
else:
    stu.display_data_dates(dt_from, dt_to)
    selected_dates = st.multiselect('Dates', dts, dts)
    tokens = df.token.unique()
    # return All_df[All_df.categ == categ].token.unique()
    selected_tokens = st.multiselect('Error Tokens', tokens)
    with st.spinner("Please Wait ... "):
        log_stats = logs.get_df_data(df, selected_dates, selected_tokens)
    if not selected_dates:
        st.markdown (f'#### Log data during {dt_from} to {dt_to}')
    else:
        st.markdown (f'#### Log data during {selected_dates[0]} to {selected_dates[-1]}')
    st.dataframe(log_stats)
    col1, col2 = st.columns(2)
    if col2.button(label = 'Save ResData'):
        res_data = df.loc[df.service.isin( ('confirmReservation', 'confirmLandReservation'))]
        if res_data.size > 0:
            # ("df.dt.unique()", df.dt.unique())
            proj_date = df.dt.unique()[0]
            # proj_date = f"{dt[8:]}-{dt[5:7]}-{dt[2:4]}"
            # ('proj_date', proj_date)
            project = db.query_to_list(f"SELECT project_id, project_type_id FROM project WHERE start_date = '{proj_date}'", return_header=False)
            # project           
            if len(project) > 0:
                project_id = project[0][0] 
                project_type = project[0][1]
            else:
                project_id = 0
                project_type = 0
            res_data['proj_id'] = project_id
            conn, _ = db.open_db()
            if project_type in (1,3):   # units
                res_data.to_sql('log_res_unit', if_exists="append", con=conn)
            else:
                res_data.to_sql('log_res_land', if_exists="append", con=conn)
            conn.close()
            st.info("Done ...")
            # col2.download_button(label = 'Res. Data', data = stu.convert_df(res_data), file_name = f'Res. Data_{selected_dates[0]}.csv', mime = 'text/csv')