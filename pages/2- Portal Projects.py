import streamlit as st
import plogs as logs 
import DB as db
import Home as stu
import pandas as pd

def proj_stats():
    df = db.query_to_pd('project')
    df = df.set_index('project_id', drop=True).sort_values('project_id', ascending=False).drop(columns=['index'])
    # df.columns
    df[df.columns[7:12]]  = df[df.columns[7:12]].fillna(0).astype(int)
    # df = df.drop(columns=['Unnamed: 3', 'Unnamed: 11', 'Unnamed: 13'], axis=1)

    df = df\
        .assign(pcnt_res_to_paid= (df['No. of reservations']/df['No. of paid customers']*100).fillna(0).astype(int))\
        .assign(pcnt_paid_to_apps= (df['No. of paid customers']/df['No. of applications']*100).fillna(0).astype(int))\
        .assign(paid_to_units_lands= (df['No. of paid customers']/df['No. of units/lands']*100).fillna(0).astype(int))
    
    return df

def portal_projects():
    
    df = proj_stats()
    # df.columns
    # df
    col_list = [0, 1, 6, 7,8,9,10,11,12]
    stats = df[df.columns[col_list]]#.set_index('project_id')
    

    # st.dataframe(stats.groupby('project_type_name_ar').count().columns)
    # st.dataframe(stats.groupby('project_type_name_ar').count()['project_name_ar'])

    stats_2 = stats.drop(columns=['project_name_ar', 'pcnt_paid_to_apps', 'pcnt_paid_to_apps', 'pcnt_res_to_paid' ])\
        .groupby('project_type_name_ar').sum().astype(int)\
        .assign(no_projects = df.groupby('project_type_name_ar').count()['project_name_ar'])\
    
    stats_2.loc['الإجمالى'] = stats_2.sum() 

    stats_2 = stats_2\
        .assign(pcnt_res_to_paid= lambda x:(x['No. of reservations']/x['No. of paid customers']*100).fillna(0).astype(int))\
        .assign(pcnt_paid_to_apps= lambda x: (x['No. of paid customers']/x['No. of applications']*100).fillna(0).astype(int))\
        .assign(paid_to_units_lands= lambda x: (x['No. of paid customers']/x['No. of units/lands']*100).fillna(0).astype(int))

    
    stats_2 = stats_2.rename({
        'no_projects':'عدد المشاريع',
        'No. of units/lands': 'عدد الاراضى/الوحدات المطروحة',
        'No. of applications': 'إجمالى عدد الإستمارات',
        'No. of paid customers': 'عدد من سدد جدية الحجز',
        'No. of reservations': 'عدد الحجوزات',
        'paid_to_units_lands': '% جدية الحجز الى الوحدات المطروحة',
        'pcnt_paid_to_apps': '% جدية الحجز الى الاستمارات',
        'pcnt_res_to_paid': '% الحجز الى من سدد',
    }, axis=1)
    
    
    stats_2 = stats_2[stats_2.columns[[5,0,1,2,3,4,6,7]]].T     # reorder and transpose
    # stats_2 = stats.style.format({'% جدية الحجز الى الاستمارات':'{:.0f}%'})
    # st.dataframe(stats_2.T)
    ## Add % sign to 2 pcnt fields 
    for i in range (5,8):
        for j in range(0,4):
            stats_2.iloc[i,j] = f"{stats_2.iloc[i,j]}%"

    st.subheader("Reservation Portal Stats as of "+ logs.get_last_proj_rep_date()[:19])
    st.dataframe(stats_2.astype(str))

    st.subheader("Reservation Portal Stats-Details")
    st.dataframe(stats, column_config={
        'no_projects':'عدد المشاريع',
        'No. of units/lands': 'عدد الاراضى/الوحدات المطروحة',
        'No. of applications': 'إجمالى عدد الإستمارات',
        'No. of paid customers': 'عدد من سدد جدية الحجز',
        'No. of reservations': 'عدد الحجوزات',
        'paid_to_units_lands': st.column_config.NumberColumn(
            '% جدية الحجز الى الوحدات المطروحة', format= "%d%%"),
        'pcnt_paid_to_apps': st.column_config.NumberColumn(
            '% جدية الحجز الى الاستمارات', format= "%d%%"),
        'pcnt_res_to_paid': st.column_config.NumberColumn(
            '% الحجز الى من سدد', format= "%d%%")
        })

def open_projects():
    sql = "SELECT * FROM project WHERE publish_date <= CURRENT_DATE AND publish_end_date >= CURRENT_DATE"
    df = db.query_to_pd(sql).drop(columns='index').set_index('project_name_ar')
    
    st.subheader("Status of Open Projects as of "+ logs.get_last_proj_rep_date()[:19])
   

    # ------------------------------------------------------------------------------------
    dfall = proj_stats()
    dfall= dfall.\
        loc[dfall.project_type_name_ar == df.project_type_name_ar[0]] \
        [['No. of reservations','No. of paid customers', 'No. of applications', 'No. of units/lands']]\
        .sum().astype(int)
    dfall['pcnt_res_to_paid']= int(dfall['No. of reservations']/dfall['No. of paid customers']*100)
    dfall['pcnt_paid_to_apps']= int(dfall['No. of paid customers']/dfall['No. of applications']*100)
    dfall['paid_to_units_lands']= int (dfall['No. of paid customers']/dfall['No. of units/lands']*100)
    
    st.dataframe(df.astype(str).T)
    df = df\
        .assign(pcnt_res_to_paid= lambda x:(x['No. of reservations']/x['No. of paid customers']*100).fillna(0).astype(int))\
        .assign(pcnt_paid_to_apps= lambda x: (x['No. of paid customers']/x['No. of applications']*100).fillna(0).astype(int))\
        .assign(paid_to_units_lands= lambda x: (x['No. of paid customers']/x['No. of units/lands']*100).fillna(0).astype(int))
    


    
    
    # dfall['paid_to_units_lands']
    col = st.columns(3)
    col[0].metric('% جدية الحجز الى الوحدات المطروحة', 
                  str(df.paid_to_units_lands[0])+'%', 
                  delta= str(int(df.paid_to_units_lands[0] - dfall['paid_to_units_lands']))+"%")
    col[1].metric('% جدية الحجز الى الاستمارات',
                  str(df.pcnt_paid_to_apps[0])+'%', 
                  delta= str(int(df.pcnt_paid_to_apps[0] - dfall['pcnt_paid_to_apps']))+"%")
    col[2].metric('% الحجز الى من سدد', 
                  str(df.pcnt_res_to_paid[0])+'%',
                  delta= str(int(df.pcnt_res_to_paid[0] - dfall['pcnt_res_to_paid']))+"%")
                  

open_projects()

options={'...':None, 
            '1- Portal Projects Dashboard': portal_projects,
            '2- Open Projects':open_projects,
            # '3- Units- Mismatched Checksum': None,
        }

opt = st.sidebar.selectbox("Options",options.keys())
if options[opt]: 
    options[opt]() # execute option