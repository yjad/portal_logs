import streamlit as st
import plogs as logs 
import DB as db
import Home as stu
import pandas as pd


def portal_projects():
    df = db.query_to_pd('project')
    df = df.set_index('project_id', drop=True).sort_values('project_id', ascending=False)
    st.dataframe(df)


def mismatch_checksum_lands():
    st.header("Mismatched Checksum - Lands")
    sql = """
SELECT org.*, ovr.land_id 'ovr_land_id', ovr.land_application_date 'ovr_land_application_date', 
              ovr.applicant_national_id 'ovr_national_id' , ovr.available_lands 'ovr.available_lands'  
FROM 
(
SELECT app.project_id,land_application_date, reservation_number, NID,
	--land_application_status,
	app.land_id,  
	land.land_no, land.land_size, land.excellence_ratio, land.available_lands, 
	--app.check_sum,--log.checksum,
	log.land_no 'log_land_no', log.land_size 'log_land_size',log.excellence_ratio 'log_excellence_ratio', log.Sub_District 'log_sub_district'
FROM land_application app Left JOIN land on app.land_id = land.land_id
					      left Join log_res_land log on log.checksum = app.check_sum
  WHERE land.land_no <> log.land_no and token = 'confirmLandReservation True' and app.project_id in (SELECT DISTINCT proj_id FROM log_res_land)
  ) as org LEFT JOIN
  (
  SELECT 	ovr_app.project_id, ovr_app.land_application_date, ovr_app.applicant_national_id, 
			ovr_land.land_id, ovr_land.land_no, ovr_land.excellence_ratio, ovr_land.land_size, ovr_land.sub_district_name_ar, ovr_land.available_lands
	FROM land_application ovr_app Left JOIN land ovr_land on ovr_app.land_id = ovr_land.land_id
  ) as ovr
  ON org.project_id = ovr.project_id and org.log_land_no = ovr.land_no and org.log_excellence_ratio= ovr.excellence_ratio 
		and org.log_land_size = ovr.land_size and org.log_sub_district = ovr.sub_district_name_ar
  ORDER BY project_id, reservation_number, land_application_date
  """
    df = db.query_to_pd(sql, table = False)
    df1= df[['project_id', 'land_application_date']].groupby(['project_id']).count()
    df1 = df1.rename({'land_application_date':'MismatchCount'}, axis=1).reset_index().set_index('project_id')
    df2 = db.query_to_pd('project').set_index('project_id')
    df3 = pd.concat([df2, df1],axis=1 )
    df3 = df3.dropna(subset=['MismatchCount'])
    df3.MismatchCount = df3.MismatchCount.astype(int)
    st.dataframe(df3)

    col1, col2 = st.columns(2)
    col1.markdown(stu.excel_download(df3, index= True, file_name = 'Land mismatched_checksum_summary.xlsx', link_display_title="Download summary"), unsafe_allow_html=True)
    col2.markdown(stu.excel_download(df, index= True, file_name = 'Land mismatched_checksum details.xlsx', link_display_title="Download Details"), unsafe_allow_html=True)


def mismatch_checksum_units():
    st.header("Mismatched Checksum - Units")
    sql = """
SELECT org.*, ovr.unit_id 'over_land_id', ovr.unit_application_date 'ovr_unit_application_date' , 
			  ovr.applicant_national_id 'ovr_applicant_national_id', ovr.available_units 'ovr_available_units' 
FROM 
(
SELECT app.project_id,unit_application_date, reservation_number, NID,
	app.unit_id,  
	unit.unit_no, unit.building_no,  unit.available_units, 
	log.unit_no 'log_unit_no', log.building_no 'log_building_no', log.Sub_District 'log_sub_district'
FROM unit_application app Left JOIN unit on app.unit_id = unit.unit_id
					      left Join log_res_unit log on log.checksum = app.check_sum
  WHERE unit.unit_no <> log.unit_no and token = 'confirmunitReservation True' and app.project_id in (SELECT DISTINCT proj_id FROM log_res_unit)
  ) as org LEFT JOIN
  (
  SELECT 	ovr_app.project_id, ovr_app.unit_application_date, ovr_app.applicant_national_id, 
			ovr_unit.unit_id, ovr_unit.unit_no,  ovr_unit.building_no, ovr_unit.sub_district_name_ar, ovr_unit.available_units
	FROM unit_application ovr_app Left JOIN unit ovr_unit on ovr_app.unit_id = ovr_unit.unit_id
  ) as ovr
  ON org.project_id = ovr.project_id and org.log_unit_no = ovr.unit_no
		and org.log_building_no = ovr.building_no and org.log_sub_district = ovr.sub_district_name_ar
  ORDER BY project_id, reservation_number, unit_application_date
  """
    df = db.query_to_pd(sql, table = False)
    df1= df[['project_id', 'unit_application_date']].groupby(['project_id']).count()
    df1 = df1.rename({'unit_application_date':'MismatchCount'}, axis=1).reset_index().set_index('project_id')
    df2 = db.query_to_pd('project').set_index('project_id')
    df3 = pd.concat([df2, df1],axis=1 )
    df3 = df3.dropna(subset=['MismatchCount'])
    df3.MismatchCount = df3.MismatchCount.astype(int)
    st.dataframe(df3)

    col1, col2 = st.columns(2)
    col1.markdown(stu.excel_download(df3, index= True, file_name = 'Unit mismatched_checksum_summary.xlsx', link_display_title="Download summary"), unsafe_allow_html=True)
    col2.markdown(stu.excel_download(df, index= True, file_name = 'Unit mismatched_checksum details.xlsx', link_display_title="Download Details"), unsafe_allow_html=True)



options={'...':None, 
            '1- Potal Projects': portal_projects,
            '2- Lands- Mismatched Checksum': mismatch_checksum_lands,
            '3- Units- Mismatched Checksum': mismatch_checksum_units,
        }

opt = st.sidebar.selectbox("Options",options.keys())
if options[opt]: 
    options[opt]() # execute option