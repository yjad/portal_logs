import pandas as pd

Tokens = pd.DataFrame(columns = ['token','categ','prio','service', 'func'], 
data=[
['ERROR [eg.intercom.hdb.rer.web.controllers.advice.ControllerExceptionHandler]', 'tech', 0, 'Exception', 'error_exception_handler'],
['ERROR [org.thymeleaf.TemplateEngine]', 'tech', 1, 'thymeleaf', 'thymeleaf_log_handler'],
['ERROR [io.undertow.request]', 'tech', 1, 'TemplateInputException', 'undertow_log_handler'],
['ERROR [eg.intercom.hdb.rer.web.config.security.CustomUsernamePasswordAuthenticationFilter]', 'user', 1, 'Authentication', 'authetication_error_handler'],
])

def error_exception_handler(line_no, txt):
    # found = 'ERROR [eg.intercom.hdb.rer.web.controllers.advice.ControllerExceptionHandler]'
    # Handling exception of type: org.springframework.web.HttpRequestMethodNotSupportedException: Request method 'GET' not supported
    lst = txt.split(': ')
    l = len(lst)
    try:
        if l ==1:   #(default task-7437) handling NotFoundException"
            exception_type = lst[0].split()[-1] 
            exception_desc = ''
        elif l == 2:
            # (default task-7604) Handling exception of type: java.lang.Exception
            exception_type = "Java Lang"     #lst[0]
            exception_desc = lst[1].rstrip()
        else:
            exception_type = lst[1].split('.')[-1]
            exception_desc = lst[2].rstrip()
    except Exception as e:
        print (line_no, ":", len(lst) , f", Exception: {e}\n", txt )

    return exception_type, exception_desc
    

def thymeleaf_log_handler(line_no, txt):
    #(default task-11398) [THYMELEAF][default task-11398] Exception processing template "index": An error happened during template parsing (template: "ServletContext resource [/WEB-INF/views/index.html]"): org.thymeleaf.exceptions.TemplateInputException: An error happened during template parsing (template: "ServletContext resource [/WEB-INF/views/index.html]")
    x=0
    p = txt[52:].find(': ') # skip (default task-11398) [THYMELEAF][default task-11398]
   
    exception_type = txt[52:p+2] 
    exception_desc = ''
    # print (exception_type)
    
    return exception_type, exception_desc
    

def parse_tech_rec(txt, line_no, out_error, dt, log_type):
# def parse_tech_log_line(line_no, txt):
    project_id = None
    task_id = None
    found = False
    
    for token in Tokens.itertuples():
        p = txt.find(token.token) 
        if p != -1 :  # found
            found = True
            # print (token, "------->", token) 
            # error_token = token.token
            if token.func:
                l = len(token.token)
                match (token.func):
                    case 'error_exception_handler':
                        ipaddress, txt = error_exception_handler(line_no, txt[p+l:])
                        # return token.token, log_desc_type, log_desc
                        service = token.service
                        error_token = token.token
                        error_categ = token.categ

                    case 'thymeleaf_log_handler' | 'undertow_log_handler' | 'authetication_error_handler':
                        ipaddress, txt = thymeleaf_log_handler(line_no, txt[p+l:])
                        service = token.service
                        error_token = token.token
                        error_categ = token.categ
                    case _:
                        print ('Unhandled exception function:', token.func)
            else:    # token found with no token.func
                print (txt)
                found = True
                ipaddress = None
                service = None
            #    pass
            if pd.isnull(token.service):
                error_token = token.token
            else:
                error_token = token.service
            error_categ = token.categ
           
            break    # exit with the first found token
    if not found:
        ipaddress = None
        service = 'Unclassified'
        error_token = None
        error_categ = None
        txt = txt[30:]      # to be analyzed
        

    rec_lst = [dt, None, line_no, None, log_type, None, ipaddress, service, error_token, error_categ,  project_id, task_id, txt]    
    # return 'Unclassified', txt, None
    return rec_lst


