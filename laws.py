import requests
import json
import re
import psycopg2
import time
from datetime import datetime
from threading import Lock


host_db = '178.62.60.87'
port_db = 5432
name_db = 'sozd'
user_name = 'kirill'
password = 'jenya1980'
lock = Lock()
row = 0



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
        cursor.execute(
            'truncate table mrr_laws; truncate table mrr_bridge_laws_deputy;truncate table mrr_bridge_department_laws;\
            truncate table mrr_bridge_faction_laws; truncate table mrr_bridge_profile_comittees_laws; truncate table mrr_bridge_responsible_comittees_laws ')
    except Exception as err:
        write_log('ERROR', 'connect_and_update', time.ctime() + ": error " + f"{err}")
    return None, False


def insert_laws():
    try:
        conn = psycopg2.connect(
            user=user_name,
            password=password,
            host=host_db,
            port=port_db,
            database=name_db)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(law_insert_sql)
    except Exception as err:
        write_log('ERROR', 'connect_and_update', time.ctime() + ": error " + f"{err}")
    return None, False

def insert_bridge_faction_laws():
    try:
        conn = psycopg2.connect(
            user=user_name,
            password=password,
            host=host_db,
            port=port_db,
            database=name_db)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(insert_bridge_faction_law_sql)
    except Exception as err:
        write_log('ERROR', 'connect_and_update', time.ctime() + ": error " + f"{err}")
    return None, False

def insert_bridge_laws_deputy():
    try:
        conn = psycopg2.connect(
            user=user_name,
            password=password,
            host=host_db,
            port=port_db,
            database=name_db)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(insert_bridge_laws_deputy_sql)
    except Exception as err:
        write_log('ERROR', 'connect_and_update', time.ctime() + ": error " + f"{err}")
    return None, False


def insert_bridge_laws_department():
    try:
        conn = psycopg2.connect(
            user=user_name,
            password=password,
            host=host_db,
            port=port_db,
            database=name_db)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(insert_bridge_laws_department_sql)
    except Exception as err:
        write_log('ERROR', 'connect_and_update', time.ctime() + ": error " + f"{err}")
    return None, False

def insert_bridge_profile_comittees_laws():
    try:
        conn = psycopg2.connect(
            user=user_name,
            password=password,
            host=host_db,
            port=port_db,
            database=name_db)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(insert_bridge_profile_comittee_law_sql)
    except Exception as err:
        write_log('ERROR', 'connect_and_update', time.ctime() + ": error " + f"{err}")
    return None, False

def insert_bridge_responsible_comittees_laws():
    try:
        conn = psycopg2.connect(
            user=user_name,
            password=password,
            host=host_db,
            port=port_db,
            database=name_db)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(insert_bridge_responsible_comittee_law_sql)
    except Exception as err:
        write_log('ERROR', 'connect_and_update', time.ctime() + ": error " + f"{err}")
    return None, False

api_token = '102a38817f1a483cf2410bcc268ec51bf7baa01e'
app_token = 'appe6928a827a71f84553973a79e0ac07e7cb1a4560'
start_at = 0
row_count = 0
page = 1
need_finish = False
url_for_total_count = 'http://api.duma.gov.ru/api/' + api_token + '/search.json?app_token=' + app_token+'&registration_start=2020-01-01'
# response = http.get( url_for_total_count)

response = requests.request('GET', url_for_total_count)
result = json.loads(response.text)
total: int = (int(result['count']))

truncate_table()

print(total, need_finish)
while not need_finish:
    url = 'http://api.duma.gov.ru/api/' + api_token + '/search.json?app_token=' + app_token + '&limit=20&page=' + str(
        page)+'&registration_start=2020-01-01'
    response = requests.request('GET', url)
    # response = http.get(url)
    print(url)



    try:
        result = json.loads(response.text)
        if result is None or result == '{}':
            print('I got a null or empty string value for data in a file')
        else:
            laws = result['laws']
            # print(laws)

            row_count = row_count + len(laws)
            need_finish = row_count > total
            # print(laws)
            # print(need_finish)
            page = page + 1

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
                if 'stage' in last_event:
                    stage_id = last_event['stage']['id']
                    stage_name = last_event['stage']['name']
                else:
                    stage_id = 'n/a'
                    stage_name = 'n/a'
                if 'phase' in last_event:
                    phase_id = last_event['phase']['id']
                    phase_name = last_event['phase']['name']
                else:
                    phase_id = 'n/a'
                    phase_name = 'n/a'
                law_type_id = laws[n]['type']['id']
                law_type_name = laws[n]['type']['name']
                law_insert_sql = 'insert\
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
                           law_type_id,\
                           law_type_name)\
                           values(' + "'" + str(law_id) + \
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
                                 "','" + str(law_type_id) + \
                                 "','" + str(law_type_name) + \
                                 "');"
                insert_laws()
                row_count = row_count + 1
                row = row + 1
                print('inserted ', row, ' rows at ', datetime.now())
                deputies = laws[n]['subject']['deputies']
                if len(deputies) > 0:
                    for i in range(0, len(deputies)):
                        deputy_id = deputies[i]['id']
                        deputy_name = deputies[i]['name']
                        deputy_position = deputies[i]['position']
                        is_deputy_current = deputies[i]['isCurrent']
                        insert_bridge_laws_deputy_sql = 'insert\
                                                into\
                                                public.mrr_bridge_laws_deputy (deputy_id,\
                                                law_id,\
                                                deputy_position,\
                                                deputy_is_current)\
                                                values(' + "'" + str(deputy_id) + \
                                                        "','" + str(law_id) + \
                                                        "','" + str(deputy_position) + \
                                                        "','" + str(is_deputy_current) + \
                                                        "');"
                        insert_bridge_laws_deputy()

                departments = laws[n]['subject']['departments']
                if len(departments) > 0:
                    for a in range(0, len(departments)):
                        department_id = departments[a]['id']
                        insert_bridge_laws_department_sql = 'INSERT INTO public.mrr_bridge_department_laws\
                                                             (law_id, department_id)\
                                                              values(' + "'" + str(law_id) + \
                                                            "','" + str(department_id) + "');"

                        insert_bridge_laws_department()
                fractions = laws[n]['subject']['factions']
                if len(fractions) > 0:
                    for b in range(0, len(fractions)):
                        fraction_id = fractions[b]['id']
                        insert_bridge_faction_law_sql = 'INSERT INTO public.mrr_bridge_faction_laws\
                                                             (law_id, fraction_id)\
                                                              values(' + "'" + str(law_id) + \
                                                            "','" + str(fraction_id) + "');"
                        insert_bridge_faction_laws()

                profile_committees = laws[n]['committees']['profile']
                if len(profile_committees)>0:
                    for c in range(0, len(profile_committees)):
                        profile_committee_id = profile_committees[c]['id']
                        insert_bridge_profile_comittee_law_sql = 'INSERT INTO public.mrr_bridge_profile_comittees_laws\
                                                                             (law_id, profile_committee_id)\
                                                                              values(' + "'" + str(law_id) + \
                                                        "','" + str(profile_committee_id) + "');"
                        insert_bridge_profile_comittees_laws()

                responsible_committees = laws[n]['committees']['responsible']
                if responsible_committees:
                   responsible_committee_id = responsible_committees['id']
                   insert_bridge_responsible_comittee_law_sql = 'INSERT INTO public.mrr_bridge_responsible_comittees_laws\
                                                                                             (law_id, responsible_committee_id)\
                                                                                              values(' + "'" + str(law_id) + \
                                                                 "','" + str(responsible_committee_id) + "');"
                   insert_bridge_responsible_comittees_laws()
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
    except Exception as er:
        print('error is -->',f'{er}')
        # print(response.text)