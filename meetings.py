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

api_token = '102a38817f1a483cf2410bcc268ec51bf7baa01e'
app_token = 'appe6928a827a71f84553973a79e0ac07e7cb1a4560'

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
        cursor.execute('truncate table mrr_meetings')
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
page = 1
need_finish = False
meetings = {"kodez":[],"datez":[]}
url_for_total_count = 'http://api.duma.gov.ru/api/' + api_token + '/questions.json?app_token=' + app_token+'&dateFrom=2021-01-01&dateTo=2021-02-01'
# response = http.get( url_for_total_count)

response = requests.request('GET', url_for_total_count)
result = json.loads(response.text)
total: int = (int(result['totalCount']))
print(total)


print(total, need_finish)

# truncate_table()

while not need_finish:
    url = 'http://api.duma.gov.ru/api/' + api_token + '/questions.json?app_token=' + app_token + '&dateFrom=2021-01-01&dateTo=2021-02-01&page=' + str(
        page)
    print(url)
    response = requests.request('GET', url)
    result = json.loads(response.text)
    try:
        result = json.loads(response.text)
        if result is None or result == '{}':
            print('I got a null or empty string value for data in a file')
        else:
            questions = result['questions']
            row_count = row_count + len(questions)
            need_finish = row_count >= total

            page = page + 1
            for n in range(0, len(questions)):
                meetings["kodez"].append(questions[n]['kodz'])
                meetings["datez"].append(questions[n]['datez'])
                # print(total, need_finish, row_count, page)

    except Exception as er:
        print('error is -->',f'{er}')
    # print(type(meetings), meetings)
kodez = []
for val in meetings['kodez']:
    if val not in kodez:
        kodez.append(val)
datez = []
for val in meetings['datez']:
    if val not in datez:
        datez.append(val)

meetings['kodez'] = kodez
meetings['datez'] = datez

for i in range(0, len(meetings['kodez'])):
   kode = meetings['kodez'][i]
   date = meetings['datez'][i]
   insert_sql = 'INSERT INTO public.mrr_meetings\
                                     (kodez, dataz)\
                                                                              values(' + "'" + str(kode) + \
                                                        "','" + str(datez) + "');"
   connect()