import requests
import json
import re
import psycopg2
import time
from threading import Lock

host_db = '178.62.60.87'
port_db = 5432
name_db = 'sozd'
user_name = 'postgres'
password = 'password'
lock = Lock()


def write_log(level: str, src: str, msg: str, with_out_lf=False):
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
url_for_total_count = 'http://api.duma.gov.ru/api/' + api_token + '/search.json?app_token=' + app_token
response = requests.request('GET', url_for_total_count)
result = json.loads(response.text)
total = (int(result['count']))
# truncate_table()
pages_number = total / 20

# print(total)
while not need_finish:
    url = 'http://api.duma.gov.ru/api/' + api_token + '/search.json?app_token=' + app_token + '&limit=20&page=' + str(
        page)
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
        event_date = last_event['date']
        for event in last_event:
            for stage in event:
                stage_id = last_event['stage']['id']
                stage_name = last_event['stage']['name']
            for phase in event:
                phase_id = last_event['phase']['id']
                phase_name = last_event['phase']['name']
        deputies = laws[n]['subject']['deputies']
        if len(deputies) > 0:
            for i in range(0, len(deputies)):
                deputy_id = deputies[i]['id']
                deputy_name = deputies[i]['name']
                deputy_position = deputies[i]['position']
                is_deputy_current = deputies[i]['isCurrent']
        else:
            deputy_id = 'n/a'
            deputy_name = 'n/a'
            deputy_position = 'n/a'
            is_deputy_current = 'n/a'
        departments = laws[n]['subject']['departments']
        if len(departments) > 0:
            for a in range(0, len(departments)):
                department_id = departments[a]['id']
                department_name = departments[a]['name']
                department_isCurrent = departments[a]['isCurrent']
                department_startDate = departments[a]['startDate']
                department_endDate = departments[a]['endDate']
        else:
            department_id = 'n/a'
            department_name = 'n/a'
            department_isCurrent = 'n/a'
            department_startDate = 'n/a'
            department_endDate = 'n/a'

        fractions = laws[n]['subject']['factions']
        if len(fractions) > 0:
            for b in range(0, len(fractions)):
                fraction_id = fractions[b]['id']
                fraction_name = fractions[b]['name']
        else:
            fraction_id = 'n/a'
            fraction_name = 'n/a'
        committees = laws[n]['committees']['responsible']
        if committees:
            comitee_id = committees['id']
            comitee_name = committees['name']
        else:
            comitee_id = 'n/a'
            comitee_name = 'n/a'
        profiles = laws[n]['committees']['profile']
        if len(profiles) > 0:
            for d in range(0, len(profiles)):
                profile_id = profiles[d]['id']
                profile_name = profiles[d]['name']
                profile_is_current = profiles[d]['isCurrent']
                profile_startDate = profiles[d]['startDate']
                profile_endDate = profiles[d]['endDate']
        else:
            profile_id = 'n/a'
            profile_name = 'n/a'
            profile_is_current = 'n/a'
            profile_startDate = 'n/a'
            profile_endDate = 'n/a'
        soexecutor = laws[n]['committees']['soexecutor']
        if len(soexecutor) > 0:
            for e in range(0, len(soexecutor)):
                soexecutor_id = soexecutor[e]['id']
                soexecutor_name = soexecutor[e]['name']
                soexecutor_isCurrent = soexecutor[e]['isCurrent']
                soexecutor_startDate = soexecutor[e]['startDate']
                soexecutor_endDate = soexecutor[e]['endDate']
        else:
            soexecutor_id = 'n/a'
            soexecutor_name = 'n/a'
            soexecutor_isCurrent = 'n/a'
            soexecutor_startDate = 'n/a'
            soexecutor_endDate = 'n/a'
        law_type_id = laws[n]['type']['id']
        law_type_name = laws[n]['type']['name']

        insert_sql = "insert\
            into\
            public.mrr_laws (law_id,\
            law_number,\
            law_name,\
            law_comments,\
            law_introductiondate,\
            law_url,\
            event_solution,\
            event_date,\
            stage_id,\
            stage_name,\
            phase_id,\
            phase_name,\
            deputy_id,\
            deputy_position,\
            deputy_name,\
            is_deputy_current,\
            department_id,\
            department_name,\
            department_iscurrent,\
            department_startdate,\
            department_enddate,\
            fraction_id,\
            fraction_name,\
            comitee_id,\
            comitee_name,\
            profile_id,\
            profile_name,\
            profile_is_current,\
            profile_startdate,\
            profile_enddate,\
            soexecutor_id,\
            soexecutor_name,\
            soexecutor_iscurrent,\
            soexecutor_startdate,\
            soexecutor_enddate,\
            law_type_id,\
            law_type_name)\
            values(" + "'" + str(law_id) + \
                     "','" + str(law_number) + \
                     "','" + str(law_name) + \
                     "','" + str(law_comments) + \
                     "','" + str(law_introductionDate) + \
                     "','" + str(law_url) + \
                     "','" + str(event_solution) + \
                     "','" + str(event_date) + \
                     "','" + str(stage_id) + \
                     "','" + str(stage_name) + \
                     "','" + str(phase_id) + \
                     "','" + str(phase_name) + \
                     "','" + str(deputy_id) + \
                     "','" + str(deputy_position) + \
                     "','" + str(is_deputy_current) + \
                     "','" + str(deputy_name) + \
                     "','" + str(department_id) + \
                     "','" + str(department_name) + \
                     "','" + str(department_isCurrent) + \
                     "','" + str(department_startDate) + \
                     "','" + str(department_endDate) + \
                     "','" + str(fraction_id) + \
                     "','" + str(fraction_name) + \
                     "','" + str(comitee_id) + \
                     "','" + str(comitee_name) + \
                     "','" + str(profile_id) + \
                     "','" + str(profile_name) + \
                     "','" + str(profile_is_current) + \
                     "','" + str(profile_startDate) + \
                     "','" + str(profile_endDate) + \
                     "','" + str(soexecutor_id) + \
                     "','" + str(soexecutor_name) + \
                     "','" + str(soexecutor_isCurrent) + \
                     "','" + str(soexecutor_startDate) + \
                     "','" + str(soexecutor_endDate) + \
                     "','" + str(law_type_id) + \
                     "','" + str(law_type_name) + \
                      "');"

        # print(insert_sql)
        connect()