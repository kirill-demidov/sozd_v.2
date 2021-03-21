import requests
from requests.auth import HTTPBasicAuth
import json
import psycopg2


def jira_users():
    result = "all finished OK"
    try:

        connection = psycopg2.connect(user = "postgres",
                                          password = "password",
                                          host = "127.0.0.1",
                                          port = "5432",
                                          database = "postgres")
        connection.autocommit=True
        cursor = connection.cursor()
        url = "https://alterosmart.atlassian.net/rest/api/3/users/search?maxResults=100"

        auth = HTTPBasicAuth("k.demidov@alterosmart.com", "L99Ib8xsuFJKtvTn8SpM8F3C")
        headers = {
           "Accept": "application/json"
        }

        response = requests.request(
           "GET",
           url,
           headers=headers,
           auth=auth
        )
        users = json.loads(response.text)
        truncate_table = 'truncate table public.mrr_users'
        cursor.execute(truncate_table)

        for user in users:
        #     print(user)
            accountType = (user['accountType'])
            display_name = (user['displayName'])
            status = (user['active'])
            userid=(user['accountId'])
            insert_users = "insert into public.mrr_users (accounttype,display_name,status,userid) values("+"'"+accountType+"','"+display_name+"','"+str(status)+"','"+userid+"')"
        #     print(insert_users)
            cursor.execute(insert_users)
    except Exception as e:
        result = "error "+f"{e}"
    return result
st = jira_users()
print(st)

