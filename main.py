import users
import groups_users
import issues
import projects_roles_actors
import issues_filed
import postgres_etl
import psycopg2
from requests.auth import HTTPBasicAuth
import time
import datetime
import issues_worklog
from telegram_send import send
import Scripts_DB_Oblects


def get_value_time(t):
    return t.hour * 3600 + t.minute * 60 + t.second + t.microsecond // 1000 / 1000

def print_dest(t_st):
    t_end = datetime.datetime.now()
    t = get_value_time(t_end) - get_value_time(t_st)
    return 'T pipeline = ' + "%.3f" % t + ' sec'

#test
connection = psycopg2.connect(user = "postgres",
                                              password = "password",
                                              host = "bi-postgres",
                                              port = "5432",
                                              database = "postgres")
connection.autocommit=True
cursor = connection.cursor()
try:
    cursor.execute(Scripts_DB_Oblects.txt_scripts)#check existing db objects and createing missing objects
except Exception as err:
        # print ("error "+f"{err}")
        send(messages=[time.ctime()+": error "+f"{err}"])
        exit(-5)
auth = HTTPBasicAuth("k.demidov@alterosmart.com", "L99Ib8xsuFJKtvTn8SpM8F3C")

error = False
t_0 = datetime.datetime.now()
t_st = datetime.datetime.now()
error, result = users.jira_users(auth,connection)
print(time.ctime()," users: " + result + print_dest(t_st))
# send(messages=[time.ctime()," users: " + result + print_dest(t_st)])
if not error:
    t_st = datetime.datetime.now()
    error,result = groups_users.group_users(auth,connection)
    print(time.ctime()," groups_users: " + result + print_dest(t_st))
    # send(messages=[time.ctime()," groups_users: " + result + print_dest(t_st)])
    if not error:
         t_st = datetime.datetime.now()
         error,result = projects_roles_actors.project_roles_actors(auth,connection)
         print(time.ctime()," projects_roles_actors: " + result + print_dest(t_st))
        #  send(messages=[time.ctime(), " projects_roles_actors: " + result + print_dest(t_st)])
         if not error:
             t_st = datetime.datetime.now()
             error, result = issues_filed.issues_fields(auth, connection)
             print(time.ctime(), " issues_filed: " + result + print_dest(t_st))
            #  send(messages=[time.ctime(), " issues_filed: " + result + print_dest(t_st)])
             if not error:
                 t_st = datetime.datetime.now()
                 error, result = issues.issues(auth, connection)
                 print(time.ctime(), " issues: " + result + print_dest(t_st))
                #  send(messages=[time.ctime(), " issues: " + result + print_dest(t_st)])
                 if not error:
                     t_st = datetime.datetime.now()
                     error, result = issues_worklog.worklog(auth, connection)
                     print(time.ctime(), " worklog: " + result + print_dest(t_st))
                    #  send(messages=[time.ctime(), " worklog: " + result + print_dest(t_st)])
                     if not error:
                         t_st = datetime.datetime.now()
                         error, result = postgres_etl.postgres_etl(connection)
                         print(time.ctime(), " postgres_etl: " + result + print_dest(t_st))
                         send(messages=[time.ctime(), " postgres_etl: " + result + print_dest(t_st)])
                         print(time.ctime()," main time: " + result + print_dest(t_0))
                         send(messages=[time.ctime(), " main time: " + result + print_dest(t_st)])
