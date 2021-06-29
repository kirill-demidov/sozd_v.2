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


def connect(insert_sql):
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
# meetings = {"kodez":[],"datez":[]}
mas = []
url_for_total_count = 'http://api.duma.gov.ru/api/' + api_token + '/questions.json?app_token=' + app_token\
                        +'&dateFrom=1994-01-01&dateTo=2021-07-01'
# response = http.get( url_for_total_count)

response = requests.request('GET', url_for_total_count)
result = json.loads(response.text)
total: int = (int(result['totalCount']))
page_size: int = (int(result['pageSize']))
page_count = (total + page_size - 1) // page_size
print(total, need_finish)

# truncate_table()

while not need_finish:
    url = 'http://api.duma.gov.ru/api/' + api_token + '/questions.json?app_token=' + app_token\
                                        + '&dateFrom=1994-01-01&dateTo=2021-07-01&page=' + str(page)
    print(url)
    try:
        response = requests.request('GET', url, timeout=30)
        result = json.loads(response.text)
        if result is None or result == '{}':
            print('I got a null or empty string value for data in a file')
        else:
            questions = result['questions']
            row_count = row_count + len(questions)
            for n in range(0, len(questions)):
                st = str(questions[n]['kodz'])+ ';'+str(questions[n]['datez'])
                if st not in mas:
                    mas.append(st)
    except Exception as er:
        print('error is -->',f'{er}',response.text)
    page = page + 1
    need_finish = page > page_count
# print(mas)
for par in mas:
   print(par.split(';')[0],par.split(';')[1] )
   insert_sql = 'INSERT INTO public.mrr_meetings (kodez, dataz)\
                 values(' + "'" + str(par.split(';')[0]) + "','" + str(par.split(';')[1]) + "');"
   connect(insert_sql)