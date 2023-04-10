import streamlit as st
import plogs as logs 
import Home as stu




def summarize_log_file():
    uploaded_files= st.file_uploader('Select Log file',type=["zip", 'gz'], accept_multiple_files = False)
    if st.button('Process ...') and uploaded_files:
        with st.spinner("Please Wait ... "):
            logs.summerize_portal_logs(uploaded_files, load_db=False)
        st.success("File extraced ...")

def Summarize_big_file():
    filename = st.text_input(label = "Enter file path:")
    if st.button('Process ...'):
        with st.spinner("Please Wait ... "):
            logs.summerize_portal_logs(filename, load_db=False)
        st.success("File extraced ...")


def summarize_log_exceptions_file():
    uploaded_files= st.file_uploader('Select Log file',type=["log"], accept_multiple_files = False)
    if st.button('Process ...') and uploaded_files:
        with st.spinner("Please Wait ... "):
            logs.summerize_exception_file(uploaded_files)
    st.success("File extraced ...")

def load_db_project_table():
    uploaded_file= st.file_uploader('Select project file',type=["csv"], accept_multiple_files = False)
    if st.button('Process ...') and uploaded_file:
        with st.spinner("Please Wait ... "):
            logs.load_project_table(uploaded_file)
    st.success("File Loaded ...")

def quote_log_file():
    file_size_option = st.radio("File Size: ",["< 200 MB", "Big file"], horizontal = True)
    if file_size_option == "< 200 MB":
        uploaded_file= st.file_uploader('Select Log file',type=["zip", 'gz'], accept_multiple_files = False)
        # uploaded_file_type = 'UPLOAD'
    else:
        uploaded_file = st.text_input(label = "Enter file path:")
        # uploaded_file_type = str

    option = st.radio("Optios: ",["Quote range", "Quote string"] , horizontal = True)
    if option == "Quote range":
        from_line_no = st.number_input('From Line_no: ', step=1)
        to_line_no   = st.number_input('to   Line_no: ', step=1)
        quote = None
        ioption = 1
        out_file_name = f"Log file range{from_line_no}-{to_line_no}.csv"
    elif option == "Quote string":
        quote = st.text_input('Quote to search: ')
        from_line_no = to_line_no= None
        ioption = 2
        out_file_name = f"Log file quote- {quote}.csv"


    if st.button('Process ...') and uploaded_file:
        with st.spinner("Please Wait ... "):
            df = logs.quote_log_file(uploaded_file, option = ioption, quote = quote, from_line = from_line_no, to_line = to_line_no)
            if df.size == 0: 
                st.warning("No data found ...")
                return 
            st.write ("No of lines: ", len(df))
            st.write (df[:20])
            df = df.to_csv().encode('utf-8')
        
        st.download_button(label = 'Save Quote', data =df, file_name = out_file_name, mime = 'text/csv')
    # elif option == "Quote string":
    #     quote = st.text_input('Quote to search: ')
    #     if st.button('Process ...') and uploaded_file:
    #         with st.spinner("Please Wait ... "):
    #             lst = logs.quote_log_file(uploaded_file, option = 2, quote = quote, from_line = None, to_line = None)
            
    #         st.write ("No of lines: ", len(lst))
    #         st.write (lst[:20])
    #         # st.download_button(label = 'Save Quote', data = lst, file_name = 'Login By Country.txt', mime = 'text/csv')



options={   '...':None, 
            '1- Summarize log file': summarize_log_file,
            '2- Summarize big-size log file (> 200GB)': Summarize_big_file,
            '3- Summarize log exception file': summarize_log_exceptions_file,
            "4- Load Portal Projects' Data": load_db_project_table, 
            "5- Quote log file": quote_log_file 
        }

opt = st.sidebar.selectbox("Options",options.keys())
if options[opt]: 
    options[opt]() # execute option
