import requests
from requests.auth import HTTPBasicAuth
import json
import psycopg2


def issues_fields(auth, connection):
    error = False
    result = "all finished OK"
    try:


        cursor = connection.cursor()
        url = "https://alterosmart.atlassian.net/rest/api/3/field"


        headers = {
           "Accept": "application/json"
        }

        response = requests.request(
           "GET",
           url,
           headers=headers,
           auth=auth
        )
        issue_fields = json.loads(response.text)
        truncate_table = 'truncate table public.mrr_issue_fields'
        cursor.execute(truncate_table)

        for field in issue_fields:
#       print(field)
            field_id = (field['id'])
            field_name = (field['name'])
            insert_users = "insert into public.mrr_issue_fields (field_id,field_name) values("+"'"+field_id+"','"+field_name+"')"
#             print(insert_users)
            cursor.execute(insert_users)
    except Exception as e:
        result = "error " + f"{e}"
        error = True
    return error, result
