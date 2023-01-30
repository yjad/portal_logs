import pandas as pd

Tokens = pd.DataFrame(columns = ['token','categ','prio','desc', 'func'], 
data=[
['ERROR [eg.intercom.hdb.rer.web.controllers.advice.ControllerExceptionHandler]', 'tech', 0, 'Exception', 'error_exception_handler'],
['ERROR [org.thymeleaf.TemplateEngine]', 'tech', 1, 'thymeleaf', 'thymeleaf_log_handler'],
['ERROR [io.undertow.request]', 'tech', 1, 'TemplateInputException', 'undertow_log_handler'],
# ['ERROR [eg.intercom.hdb.rer.web.config.WebRequestInterceptor]', 'user', 1, 'NID data', 'WebRequestInterceptor'],
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
    

def thymeleaf_log_handler(txt):
    return 0, 0
    # x=0
    # log_type = txt[30:x+1] # skip (default task-9999)
    # log_desc = txt[x+22:].lstrip()
    # if log_type == '[org.thymeleaf.TemplateEngine]': 
    #     log_desc = log_desc[31:].lstrip()    # skip task-id in [THYMELEAF][default task-10430]
    # x = log_desc.find('org.springframework.')
    # if x != -1: # found
    #     y = log_desc[x:].find(':')
    #     log_desc_type = log_desc[x:x+y].split('.')[-1]
    #     print (y, log_desc[x:x+y+1], "***", log_desc_type)
    # else:
    #     log_desc_type = ''
    # pass

def parse_tech_rec(txt, line_no, out_error, dt, log_type):
# def parse_tech_log_line(line_no, txt):
    project_id = None
    task_id = None
    for token in Tokens.itertuples():
        p = txt.find(token.token) 
        if p != -1 :  # found
            # print (token, "------->", token) 
            # error_token = token.token
            if token.func:
                l = len(token.token)
                match (token.func):
                    case 'error_exception_handler':
                        log_desc_type, log_desc = error_exception_handler(line_no, txt[p+l:])
                        # return token.token, log_desc_type, log_desc
                    case 'thymeleaf_log_handler':
                        return token.token, token.desc, ''
                    case 'undertow_log_handler':
                        return token.token, token.desc, ''
                    case 'WebRequestInterceptor':
                        return token.token, token.desc, ''
                    case 'authetication_error_handler':
                        return token.token, token.desc, ''
            else:    
                if pd.isnull(token.desc):
                    error_token = token.token
                else:
                    error_token = token.desc
                error_categ = token.categ

        # txt= None   # error text is not needed in this case
        classified = True
        break    
    rec_lst = [dt, None, line_no, None, log_type, None, None, service, error_token, error_categ,  project_id, task_id, txt]    
    # return 'Unclassified', txt, None
    return rec_lst

