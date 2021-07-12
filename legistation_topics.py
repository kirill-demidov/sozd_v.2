import requests
import json
import psycopg2
import time
from threading import Lock


host_db = '127.0.0.1'
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

truncate_table = "truncate table mrr_legistation_topics;"
sql_query(truncate_table)
print('table mrr_legistation_topics is truncated')

url =  'http://api.duma.gov.ru/api/'+api_token+'/topics.json?app_token='+app_token
response = requests.request('GET',url)
result = json.loads(response.text)

for topic in range(0,len(result)):
    topic_id = result[topic]['id']
    topic_name = result[topic]['name']
    insert_data = "insert into mrr_legistation_topics (id, topic_name)\
                  VALUES("+ "'" + str(topic_id) + "','" + str(topic_name)+"');"
    sql_query(insert_data)
print("table mrr_legistation_topics is updated")