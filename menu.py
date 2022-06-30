from base64 import decode
from numpy import true_divide
import streamlit as st
from streamlit_option_menu import option_menu
import plogs as logs 
import matplotlib.pyplot as plt


st.title('Reservation Portal Logs - followup')
def plot_1():
    df = logs.export_email_quota_graph().fillna(0)
    fig = df.plot(x='dt', y=['# logins', '# email quota errors'], title = 'Reservation Portal Logins vs email quota error', grid=True,
            xlabel = 'Date', ylabel = '# of customers', figsize = (10,5)).get_figure()
    return st.pyplot(fig)


with st.sidebar:
    selected = option_menu("Main Menu", ["Load Log Files", 'graphs', 'Settings'], 
        icons=['house', '', 'gear'], menu_icon="cast", default_index=1)
    # selected

match selected:
    case "Load Log Files":
        uploaded_files= st.file_uploader("Upload log",type=["txt","log"], 
                                          accept_multiple_files = True)
        # st.write(type(uploaded_files))
        if st.button('Process ...'):
            pd = logs.load_portal_logs(uploaded_files, load_db=True)
            st.dataframe(pd)

    case 'graphs':
        plot_1()

    case other:
        st.write(f"Option is {selected}")


# st.markdown("""
# This app retrieves the list of the **S&P 500** (from Wikipedia) and its corresponding **stock closing price** (year-to-date)!
# * **Python libraries:** base64, pandas, streamlit, numpy, matplotlib, seaborn
# * **Data source:** [Wikipedia](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies).
# """)

# st.sidebar.header('User Input Features')




# if st.button('Show Plots'):
#     st.header('Email Quota Limit grph')
#     plot_1()

if st.button('Dataframe'):
    # st.header('Email Quota Limit grph')
    df = logs.export_email_quota_graph()
    df.iloc[:,2] = df.iloc[:,2].fillna(0).astype('int32')
    st.dataframe(df)