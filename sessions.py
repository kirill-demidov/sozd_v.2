import requests
import json
import psycopg2
import time
from threading import Lock

host_db = '178.62.60.87'
port_db = 5432
name_db = 'sozd'
user_name = 'postgres'
password = 'password'
lock = Lock()

def write_log(level: str, src: str, msg: str, with_out_lf = False):
    st = "lvl=" + level + ' ' + 'src="' + str(src).replace('"', "'") + '" msg="' + str(msg).replace('"', "'") + '"'
    lock.acquire()
    if with_out_lf:
        print("\r" + st, end="\r")
    else:
        print(st)
    lock.release()
def truncate_table():
    try:
        conn = psycopg2.connect(
            user=user_name,
            password=password,
            host=host_db,
            port=port_db,
            database=name_db)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute('truncate table mrr_sessions')
    except Exception as err:
        write_log('ERROR', 'connect_and_update', time.ctime() + ": error " + f"{err}")
    return None, False


def connect():
    try:
        conn = psycopg2.connect(
            user=user_name,
            password=password,
            host=host_db,
            port=port_db,
            database=name_db)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(insert_sql)
    except Exception as err:
        write_log('ERROR', 'connect_and_update', time.ctime() + ": error " + f"{err}")
    return None, False

api_token = '102a38817f1a483cf2410bcc268ec51bf7baa01e'
app_token = 'appe6928a827a71f84553973a79e0ac07e7cb1a4560'
truncate_table()
url =  'http://api.duma.gov.ru/api/'+api_token+'/periods.json?app_token='+app_token
response = requests.request('GET',url)
result = json.loads(response.text)
# print(url)
for duma in result:
    duma_id = duma['id']
    duma_name = duma['name']
    duma_startDate = duma['startDate']
    duma_endDate = duma['endDate']
    sessions = duma['sessions']
    for n in range(0, len(sessions)):
        session_id = sessions[n]['id']
        session_name = sessions[n]['name']
        session_startDate = sessions[n]['startDate']
        session_endDate = sessions[n]['endDate']
        insert_sql = "INSERT INTO public.mrr_sessions\
                (duma_id, duma_name, duma_startdate, duma_enddate,\
                session_id, session_name, session_startdate, session_enddate)\
                VALUES (" + "'" + str(duma_id) + "','" + str(duma_name) + "','" + \
                str(duma_startDate) + "','" + str(duma_endDate) + "','" + str(session_id) + \
                "','" + str(session_name) + "','" + str(session_startDate) + "','" + \
                str(session_endDate) + "');"
    # print(insert_sql)
        connect()