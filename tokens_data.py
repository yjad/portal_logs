import pandas as pd
Tokens = pd.DataFrame(columns = ['token','categ','prio','desc'], 
data=[
['userLocked','user',0,None],
['MailSendException','user',1,'Email quota exception'],
['UserNotFound','user',2,None],
['isReCAPTCHAValid','user',3,None],
['changeUserPassword','user',4,None],
['ResourceAccessException','user',4,'sending SMS'],
['AuthenticationFailedException','user',4,'Too many login attempts'],
['HttpRequestMethodNotSupportedException','tech',4,None],
['TemplateInputException','tech',5,None],
['IllegalStateException','tech',6,None],
['[stderr], (default task-,tech',7,None],
['Exception processing template','tech',8,None],
['ControllerExceptionHandler','tech',9,None],
['[stderr], (pool-','tech',10,None],
['mimeType','tech',11,None],
['java.lang.','tech',12,None],
['java.base','tech',13,None],
['SMSStatusUpdateJobScheduler','tech',14,None],
['.jboss.','tech',15,None],
['org.wildfly.','tech', 16, 'Wildfly'],
['org.springframework.', 'tech', 17, 'SpringFramework'],
['io.undertow.', 'tech', 18, 'Undertow Web Server'],
['javax.', 'tech', 19, None],
['jdk.internal.reflect.GeneratedMethodAccessor', 'tech', 19, 'jdk'],
['eg.intercom.hdb.rer.', 'tech', 19, 'Intercom'],
['io.opentracing.contrib.jaxrs2.server.SpanFinishingFilter', 'tech', 19, 'jaxrs2'],
['deployment.hdb-rer-admin', 'tech', 19, 'Admin CSV'],
['java.net.UnknownHostException: www.google.com', 'tech', 19, 'UnknownHostException'],
])