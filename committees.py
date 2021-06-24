import requests
import json
import psycopg2
import time
from threading import Lock


host_db = '178.62.60.87'
port_db = 5432
name_db = 'sozd'
user_name = 'kirill'
password = 'jenya1980'
lock = Lock()


def write_log(level: str, src: str, msg: str, with_out_lf = False):
    st = "lvl=" + level + ' ' + 'src="' + str(src).replace('"', "'") + '" msg="' + str(msg).replace('"', "'") + '"'
    lock.acquire()
    if with_out_lf:
        print("\r" + st, end="\r")
    else:
        print(st)
    lock.release()



def sql_query(sql):
    try:
        conn = psycopg2.connect(
            user=user_name,
            password=password,
            host=host_db,
            port=port_db,
            database=name_db)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(sql)
    except Exception as err:
        write_log('ERROR', 'connect_and_update', time.ctime() + ": error " + f"{err}")
    return None, False


api_token = '102a38817f1a483cf2410bcc268ec51bf7baa01e'
app_token = 'appe6928a827a71f84553973a79e0ac07e7cb1a4560'

truncate_table = "truncate table mrr_committees;"
sql_query(truncate_table)
print('table mrr_committees is truncated')

url =  'http://api.duma.gov.ru/api/'+api_token+'/committees.json?app_token='+app_token
print(url)
response = requests.request('GET',url)
result = json.loads(response.text)
for f in range(0, len(result)):
        commettee_id = result[f]['id']
        commettee_name = result[f]['name']
        is_current = result[f]['isCurrent']
        start_date = result[f]['startDate']
        end_date = result[f]['stopDate']
        insert_data = "INSERT INTO public.mrr_committees\
        (id, comittee_name, is_current, start_date, end_date)\
       VALUES(" + "'" + str(commettee_id) + "','" + str(commettee_name) + "','" + \
                        str(is_current) + "','" + str(start_date)+ "','" + str(end_date) + "');"
        sql_query(insert_data)
        # print(insert_data)
print('table mrr_comittees is updated')