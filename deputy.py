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
url =  'http://api.duma.gov.ru/api/'+api_token+'/deputies.json?app_token='+app_token
response = requests.request('GET',url)
result = json.loads(response.text)

n = 0
for deputy in result:
    deputy_id = deputy['id']
    deputy_name = deputy['name']
    deputy_position = deputy['position']
    is_current = deputy['isCurrent']
    if 'factions' in deputy:
        factions = deputy['factions']
        for n in range(0, len(deputy['factions'])):
            fraction_name = factions[n]['name']
            fraction_id = factions[n]['id']
            fraction_startdate = factions[n]['startDate']
            fraction_enddate = factions[n]['endDate']
            insert_sql = "insert\
                into\
                public.mrr_deputy_by_fraction (deputy_id,\
                deputy_name,\
                deputy_position,\
                is_current,\
                fraction_id,\
                fraction_name,\
                fraction_startdate,\
                fraction_enddate)\
            values(" + "'" + str(deputy_id) + "','" + str(deputy_name) + "','" + \
                         str(deputy_position) + "','" + str(is_current) + "','" + str(fraction_id) + \
                         "','" + str(fraction_name) + "','" + str(fraction_startdate) + "','" + \
                         str(fraction_enddate) + "');"
            connect()
