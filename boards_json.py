import requests
import json
from requests.auth import HTTPBasicAuth

error = False
result = " all finished OK"
issue_url = 'https://alterosmart.atlassian.net/browse/'
startAt = 0
total = None
try:
    url = "https://alterosmart.atlassian.net/rest/agile/1.0/board/27/sprint/135/issue?maxResults=500"

    headers = {
        "Accept": "application/json"
    }
    auth = HTTPBasicAuth("k.demidov@alterosmart.com", "L99Ib8xsuFJKtvTn8SpM8F3C")
    needfinish = False
    while not needfinish:
        response = requests.request(
            "GET",
            url ,
            headers=headers,
            auth=auth
        )

        issues_in_sprint = json.loads(response.text)
        total = issues_in_sprint['total']
        maxResults = issues_in_sprint['maxResults']
        needfinish = startAt + maxResults > total
        #             print (total,startAt,maxResults)
        startAt = startAt + issues_in_sprint['maxResults']
        with open('personal.json', 'w') as json_file:
            json.dump(issues_in_sprint, json_file)
        # print (issues_in_sprint['issues']['id'])
        # for issue in issues_in_sprint['issues']:
        #    print(issues_in_sprint['issues']['id'])
except Exception as e:
    result = "error " + f"{e}"
    error = True
#get all boards https://alterosmart.atlassian.net/rest/agile/1.0/board
#get all sprint in board    https://alterosmart.atlassian.net/rest/agile/1.0/board/{boardId}/sprint
#get all issues in sprint    https://alterosmart.atlassian.net/rest/agile/1.0/board/{boardId}/sprint/{sprintId}/issue
# all sprints in AlteroUniversal