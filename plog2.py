import pandas as pd

Tokens = pd.DataFrame(columns = ['token','categ','prio','service', 'func'], 
data=[
['ERROR [eg.intercom.hdb.rer.web.controllers.advice.ControllerExceptionHandler] ', 'tech', 0, 'Exception', 'error_exception_handler'],
['ERROR [org.thymeleaf.TemplateEngine] ', 'tech', 1, 'thymeleaf', 'thymeleaf_log_handler'],
['ERROR [io.undertow.request] ', 'tech', 1, 'io.undertow.request', 'undertow_log_handler'],
['ERROR [eg.intercom.hdb.rer.web.config.security.CustomUsernamePasswordAuthenticationFilter] ', 'tech', 1, 'Authentication', 'authetication_error_handler'],
['ERROR [org.springframework.web.servlet.HandlerExecutionChain] ', 'tech', 1, 'ExecutionChain', 'chain_error_handler'],
])

def error_exception_handler(line_no, txt):
    # found = 'ERROR [eg.intercom.hdb.rer.web.controllers.advice.ControllerExceptionHandler]'
    # Handling exception of type: org.springframework.web.HttpRequestMethodNotSupportedException: Request method 'GET' not supported
    lst = txt.split(': ')
    l = len(lst)
    try:
        if l ==1:   #(default task-7437) handling NotFoundException"
            service = lst[0].split()[-1] 
            exception_desc = ''
        elif l == 2:
            # (default task-7604) Handling exception of type: java.lang.Exception
            service = "Java Lang"     #lst[0]
            exception_desc = lst[1].rstrip()
        else:
            service = lst[1].split('.')[-1]
            exception_desc = lst[2].rstrip()
    except Exception as e:
        print (line_no, ":", len(lst) , f", Exception: {e}\n", txt )

    return service, exception_desc
    

def thymeleaf_log_handler(line_no, txt_0):
    #(default task-11398) [THYMELEAF][default task-11398] Exception processing template "index": An error happened during template parsing (template: "ServletContext resource [/WEB-INF/views/index.html]"): org.thymeleaf.exceptions.TemplateInputException: An error happened during template parsing (template: "ServletContext resource [/WEB-INF/views/index.html]")
    #(default task-10010) [THYMELEAF][default task-10010] Exception processing template "templates/reservation/land-reservation-print": An error happened during template parsing (template: "ServletContext resource [/WEB-INF/views/[...]d-reservation-print.html]"): org.thymeleaf.exceptions.TemplateInputException: An error happened during template parsing (template: "ServletContext resource [/WEB-INF/views/[...]d-reservation-print.html]")
    st = txt_0.find('Exception')
    end = txt_0.find(': ', st) # skip (default task-11398) [THYMELEAF][default task-11398]
    service = txt_0[st:end] 
    # print (txt_0,"\n*** service:", service, "end: ", st, end)
    exception_desc = ''
    # print (service)
    
    return service, exception_desc

def undertow_log_handler(line_no, txt):
#ERROR [io.undertow.request] (default task-11397) UT005023: Exception handling request to /hdb/: org.springframework.web.util.NestedServletException: Request processing failed; nested exception is org.thymeleaf.exceptions.TemplateInputException: An error happened during template parsing (template: "ServletContext resource [/WEB-INF/views/index.html]")
#ERROR [io.undertow.request] (default task-7503) UT005023: Exception handling request to /hdb/login: java.lang.IllegalStateException: UT010019: Response already commited
    st = txt.find('org.springframework', 52) 
    if st == -1:
        st= txt.find('java.lang.', 52) 
        end = txt.find(':', st)
        service = txt[st:end].split('.')[-1]
        st = txt.rfind(': ') 
        exception_desc = txt[st+2:].rstrip()    #Response already commited
    else:
        end = txt.find(':', st)
        service = txt[st:end].split('.')[-1] 
        exception_desc = ''

    # print (txt,"\n*** service: ", service)
    
    return service, exception_desc
    
def authetication_error_handler(line_no, txt):
#[eg.intercom.hdb.rer.web.config.security.CustomUsernamePasswordAuthenticationFilter] (default task-11398) An internal error occurred while trying to authenticate the user.: org.springframework.security.authentication.InternalAuthenticationServiceException: UserNotFound
    
    st = txt.rfind(' ') # find the last token
    end = len(txt)
    service = txt[st+1:end].rstrip() 
    # print (txt,"\n*** service: ", service)
    exception_desc = ''
    # print (service)
    
    return service, exception_desc
    

def chain_error_handler(line_no, txt):
#[org.springframework.web.servlet.HandlerExecutionChain] (default task-8944) HandlerInterceptor.afterCompletion threw exception: java.lang.NullPointerException
    st = txt.rfind(' ') # find the last token
    end = len(txt)
    service = txt[st+1:end].rstrip() 
    exception_desc = ''
    
    return service, exception_desc

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
                txt = txt[p+l-1:]
                match (token.func):
                    case 'error_exception_handler':
                        service, txt = error_exception_handler(line_no, txt)

                    case 'thymeleaf_log_handler':
                        service, txt = thymeleaf_log_handler(line_no, txt)

                    case 'undertow_log_handler':
                        service, txt = undertow_log_handler(line_no, txt)

                    case 'authetication_error_handler':
                        service, txt = authetication_error_handler(line_no, txt)
                        
                    case 'chain_error_handler':
                        service, txt = chain_error_handler(line_no, txt)

                    case _:
                        service = 'Unhandled'
                        print ('Unhandled exception function:', token.func)
                error_token = token.token
                error_categ = token.categ
            else:    # token found with no token.func
                print (txt)
                found = True
                # ipaddress = None
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
        txt = txt[30:-1]      # to be analyzed, -1 to exclude \n
        

    rec_lst = [dt, None, line_no, None, log_type, None, None, service, error_token, error_categ,  project_id, task_id, txt]    
    # return 'Unclassified', txt, None
    return rec_lst


