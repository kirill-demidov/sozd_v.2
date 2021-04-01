import requests
import json


def get_data(data, caption, default=''):
    if data:
        return data[caption]
    else:
        return default


def worklog(auth,connection):
    error = False
    result = "all finished OK"
    issue_url = 'https://alterosmart.atlassian.net/browse/'
    startAt = 0
    total = None
    try:
        cursor = connection.cursor()
        url = "https://alterosmart.atlassian.net/rest/api/3/search?jql="
        headers = {
            "Accept": "application/json"
        }

        needfinish = False
        truncate_table = 'truncate table public.mrr_worklog;'
        cursor.execute(truncate_table)
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
                tmp = ''
                worklogs = json.loads(response.text)
                logs = worklogs['worklogs']
                for log in logs:
                    if len(str(log)) < 3:
                       tmp='?'
                    else:
                        updater_id = log ['updateAuthor']['accountId']
                        worklog_id = log['id']
                        create_date = log['created']
                        update_date = log['updated']
                        start_date = log['started']
                        time_spent = log['timeSpent']
                        time_spent_sec = log['timeSpentSeconds']
                        # print(issue_id,updater_id,worklog_id,create_date,update_date,start_date, time_spent,time_spent_sec)
                        insert_worklog = "INSERT INTO public.mrr_worklog (worklog_id, issue_id, \
                        create_date, update_date, start_date, time_spent, time_spent_sec, updater_id)\
                                        values(" + "'" + str(worklog_id) + "','" + str(issue_id) + "','" + str(create_date) + "','" + \
                                         str(update_date) + "','" + str(start_date) + "','" + str(time_spent) + "'\
                                           ,'" + str(time_spent_sec) + "','" + str(updater_id)  + "')"
                        cursor.execute(insert_worklog)
    except Exception as e:
        result = "error " + f"{e}"
        error = True
    return error, result