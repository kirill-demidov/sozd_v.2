import requests
import json


def jira_users(auth, connection):
    error = False
    result = "all finished OK"
    try:
        cursor = connection.cursor()
        url = "https://alterosmart.atlassian.net/rest/api/3/users/search?maxResults=100"
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
            # print(user)
            account_type = (user['accountType'])
            display_name = (user['displayName'])
            status = (user['active'])
            userid = (user['accountId'])
            insert_users = "insert into public.mrr_users (accounttype,display_name,status,userid) values(" + "'" +\
                           account_type + "','" + display_name + "','" + str(status) + "','" + userid + "')"
        #     print(insert_users)
            cursor.execute(insert_users)
    except Exception as e:
        result = "error "+f"{e}"
        error = True
    return error, result
