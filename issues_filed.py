import requests
import json


def issues_fields(auth, connection):
    error = False
    row_count = 0
    result = "all finished OK"
    try:
        cursor = connection.cursor()
        url = "https://alterosmart.atlassian.net/rest/api/3/field"
        headers = {"Accept": "application/json"}
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
            # print(field)
            field_id = (field['id'])
            field_name = (field['name'])
            insert_users = \
                "insert into public.mrr_issue_fields (field_id,field_name) values("+"'"+field_id+"','"+field_name+"')"
#             print(insert_users)
            cursor.execute(insert_users)
            row_count = row_count + 1
    except Exception as e:
        result = "error " + f"{e}"
        error = True
    result = result + '; row_count = ' + str(row_count)
    return error, result
