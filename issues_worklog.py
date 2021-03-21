import requests
from requests.auth import HTTPBasicAuth
import json
import psycopg2
import time


def get_data(data, caption, default=''):
    if data:
        return data[caption]
    else:
        return default


def issues():
    result = "all finished OK"
    issue_url = 'https://alterosmart.atlassian.net/browse/'
    startAt = 0
    total = None
    try:

        connection = psycopg2.connect(user="postgres",
                                      password="password",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")
        connection.autocommit = True
        cursor = connection.cursor()
        url = "https://alterosmart.atlassian.net/rest/api/3/search?jql="

        auth = HTTPBasicAuth("k.demidov@alterosmart.com", "L99Ib8xsuFJKtvTn8SpM8F3C")
        headers = {
            "Accept": "application/json"
        }

        needfinish = False
        while not needfinish:

            response = requests.request(
                "GET",
                url + "&startAt=" + str(startAt),
                headers=headers,
                auth=auth
            )
            issues = json.loads(response.text)
            total = issues['total']
            maxResults = issues['maxResults']
            needfinish = startAt + maxResults > total
            #             print (total,startAt,maxResults)
            startAt = startAt + issues['maxResults']

            #             print (issues['issues'])
            for issue in issues['issues']:
                issue_id = issue['id']
                url_worklog = "https://alterosmart.atlassian.net/rest/api/3/issue/" + issue_id + "/worklog"
                headers = {
                    "Accept": "application/json"
                }

                response = requests.request(
                    "GET",
                    url_worklog,
                    headers=headers,
                    auth=auth
                )
                #                 print(json.dumps(json.loads(response.text), sort_keys=True, indent=4, separators=(",", ": ")))

                worklogs = json.loads(response.text)
                logs = worklogs['worklogs']
                print(logs)

                # print (type(items))

    except Exception as e:
        result = "error " + f"{e, issue_id}"
    return result


st = issues()
print(st)