import streamlit as st
import plogs as logs 
import DB as db
import Home as stu
import pandas as pd


def portal_projects():
    df = db.query_to_pd('project')
    df = df.set_index('project_id', drop=True).sort_values('project_id', ascending=False).drop(columns=['index'])
    # df.columns
    df[df.columns[7:12]]  = df[df.columns[7:12]].fillna(0).astype(int)
    df = df.drop(columns=['Unnamed: 3', 'Unnamed: 11', 'Unnamed: 13'], axis=1)
    df = df\
        .assign(pcnt_paid_to_apps = (df['No. of reservations']/df['No. of paid customers']*100).fillna(0).astype(int))\
        .assign(pcnt_res_to_paid = (df['No. of paid customers']/df['No. of applications']*100).fillna(0).astype(int))\
        .assign(pcnt_res_to_paid = (df['No. of paid customers']/df['No. of applications']*100).fillna(0).astype(int))\
        .assign(paid_to_units_lands = (df['No. of paid customers']/df['No. of units/lands']*100).fillna(0).astype(int))
    # df.columns
    col_list = [0, 1, 6, 7,8,9,10,11,12]
    stats = df[df.columns[col_list]]#.set_index('project_id')
    st.dataframe(stats)

    # st.dataframe(stats.groupby('project_type_name_ar').count().columns)
    # st.dataframe(stats.groupby('project_type_name_ar').count()['project_name_ar'])

    stats_2 = stats.drop(columns=['project_name_ar', 'pcnt_paid_to_apps', 'pcnt_paid_to_apps', 'pcnt_res_to_paid' ]).groupby('project_type_name_ar').sum()
    stats_2 = stats_2\
    .assign(no_projects = stats.groupby('project_type_name_ar').count()['project_name_ar'])\
    .assign(pcnt_paid_to_apps = (stats_2['No. of reservations']/stats_2['No. of paid customers']*100).fillna(0).astype(int))\
    .assign(pcnt_res_to_paid = (stats_2['No. of paid customers']/stats_2['No. of applications']*100).fillna(0).astype(int))\
    .assign(paid_to_units_lands = (stats_2['No. of paid customers']/stats_2['No. of units/lands']*100).fillna(0).astype(int))

    st.dataframe(stats_2.T)

def protal_stats_1():
    pass

options={'...':None, 
            '1- Portal Projects Summary': portal_projects,
            '2- Portal Projects Stats-1':protal_stats_1,
            # '3- Units- Mismatched Checksum': None,
        }

opt = st.sidebar.selectbox("Options",options.keys())
if options[opt]: 
    options[opt]() # execute option