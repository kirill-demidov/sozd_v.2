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
url_for_total_count =  'http://api.duma.gov.ru/api/'+api_token+'/voteSearch.json?app_token='+app_token
response = requests.request('GET',url_for_total_count)
result = json.loads(response.text)
# print(result)
total = (int(result['totalCount']))
pages_number = total/100
while not need_finish:
    url = 'http://api.duma.gov.ru/api/'+api_token+'/voteSearch.json?app_token='+app_token+'&limit=3&page='+str(page)
    response = requests.request(
                    "GET",
                    url + "&startAt=" + str(start_at)
    )
    # print(url)
    result = json.loads(response.text)
    need_finish = page > pages_number
    page = page + 1
    votes = result['votes']
    if votes:
        for vote in votes:
            voteId = vote['id']
            subject = vote['subject']
            law_id = re.search(r'\d{7}-\d{1}',subject)
            if law_id:
               law_number = law_id.group()
               law_voting_flag = 1
            else:
                law_number = 'голосование не по законопроекту'
                law_voting_flag = 0
            vote_date = vote['voteDate']
            vote_count = vote['voteCount']
            vote_for = vote ['forCount']
            vote_against = vote['againstCount']
            vote_abstain = vote['abstainCount']
            vote_absent = vote['absentCount']
            vote_result_type = vote['resultType']
            is_vote_true = vote['result']
            insert_sql = "INSERT INTO public.mrr_votes\
            (voteid, sublect, law_number, law_voting_flag, vote_date, vote_count, \
            vote_for, vote_against, vote_abstain, vote_absent, vote_result_type)\
            VALUES(" + "'" + str(voteId) + "','" + str(subject) + "','" + \
                                    str(law_number) + "','" + str(law_voting_flag) + "','" + str(vote_date) + \
                                    "','" + str(vote_count) + "','" + str(vote_for) + "','" + \
                                    str(vote_against) + "','"+str(vote_abstain) + "','"+str(vote_absent) + "','"+str(vote_result_type) + "');"
            # print(vote)
    connect()
