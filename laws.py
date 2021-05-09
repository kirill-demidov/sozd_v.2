import requests
import json
import re
import psycopg2
import time
from threading import Lock


host_db = '178.62.60.87'
# host_db = 'bi-postgres'
# host_db = '127.0.0.1'
port_db = 5432
# name_db = 'postgres'
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
        cursor.execute('truncate table mrr_votes')
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
start_at = 0
row_count = 0
page = 0
need_finish = False
url_for_total_count =  'http://api.duma.gov.ru/api/'+api_token+'/search.json?app_token='+app_token
response = requests.request('GET',url_for_total_count)
result = json.loads(response.text)
total = (int(result['count']))
# truncate_table()
pages_number = total/100
# print(total)
while not need_finish:
    url = 'http://api.duma.gov.ru/api/'+api_token+'/search.json?app_token='+app_token+'&limit=20&page='+str(page)
    response = requests.request(
                    "GET",
                    url
    )
    # print(url)
    result = json.loads(response.text)
    need_finish = page > pages_number
    page = page + 1
    laws = result['laws']
    for n in range(0, len(laws)):
        law_id = laws[n]['id']
        law_number = laws[n]['number']
        law_name = laws[n]['name']
        law_comments = laws[n]['comments']
        law_introductionDate = laws[n]['introductionDate']
        law_url = laws[n]['url']
        last_event = laws[n]['lastEvent']
        event_solution = last_event['solution']
        event_date = last_event ['date']
        for event in last_event:
            for stage in event:
                stage_id = last_event['stage']['id']
            for phase in event:
                phase_id = last_event['phase']['id']
        deputies = laws[n]['subject']['deputies']
        if len(deputies)>0:
            for i in range(0, len(deputies)):
                deputy_id = deputies[i]['id']
                deputy_position = deputies[i]['position']
                is_deputy_current = deputies[i]['isCurrent']
        departments = laws[n]['subject']['departments']
        if len(departments)>0:
            for a in range(0, len(departments)):
                department_id = departments[a]['id']
                department_name = departments[a]['name']
                department_isCurrent = departments[a]['isCurrent']
                department_startDate = departments[a]['startDate']
                department_endDate = departments[a]['endDate']
        fractions = laws[n]['subject']['factions']
        if len(fractions)>0:
            for b in range(0,len(fractions)):
                fraction_id = fractions[b]['id']
