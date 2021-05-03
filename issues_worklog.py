import requests
import json
# import commonthread


def get_data(data, caption, default=None):
    if data:
        return data[caption]
    else:
        return default


def worklog(auth, connection):
    error = False
    result = "all finished OK"
    start_at = 0
    row_count =0
    try:
        cursor = connection.cursor()
        url = "https://alterosmart.atlassian.net/rest/api/3/search?jql=created>=-1w%20OR%20updated>=-1w"  # last week data load
        # url = "https://alterosmart.atlassian.net/rest/api/3/search?jql=" #full data load
        headers = {
            "Accept": "application/json"
        }

        need_finish = False
        truncate_table = 'truncate table public.mrr_worklog;'
        cursor.execute(truncate_table)
        while not need_finish:

            response = requests.request(
                "GET",
                url + "&startAt=" + str(start_at),
                headers=headers,
                auth=auth
            )
            issues = json.loads(response.text)
            total = issues['total']
            max_results = issues['maxResults']
            need_finish = start_at + max_results > total
            start_at = start_at + max_results
            for issue in issues['issues']:
                sprint_id = -1
                sprint_name = ''
                issue_id = issue['id']
                if 'fields' in issue:
                    field = issue['fields']
                    if 'customfield_10020' in field:
                        sprint = field['customfield_10020']
                        if sprint: # and (sprint != 'null'):
                            for sp in sprint:
                                if sp["state"] == 'active':
                                    sprint_id = sp['id']
                                    sprint_name = sp['name']
                                    break
                    url_worklog = "https://alterosmart.atlassian.net/rest/api/3/issue/" + issue_id + "/worklog"
                    headers = {"Accept": "application/json"}
                    response = requests.request(
                        "GET",
                        url_worklog,
                        headers=headers,
                        auth=auth
                    )
                    work_logs = json.loads(response.text)
                    logs = work_logs['worklogs']
                    for log in logs:
                        if len(str(log)) < 3:
                            continue
                        else:
                            updater_id = log['updateAuthor']['accountId']
                            worklog_id = log['id']
                            create_date = log['created']
                            update_date = log['updated']
                            start_date = log['started']
                            time_spent = log['timeSpent']
                            time_spent_sec = log['timeSpentSeconds']
                            insert_worklog = "INSERT INTO public.mrr_worklog (worklog_id, issue_id, \
                                create_date, update_date, start_date, time_spent, time_spent_sec, updater_id,\
                                active_sprint_id,active_sprint_name)\
                                values(" + "'" + str(worklog_id) + "','" + str(issue_id) + "','" + \
                                str(create_date) + "','" + str(update_date) + "','" + str(start_date) + \
                                "','" + str(time_spent) + "','" + str(time_spent_sec) + "','" + \
                                str(updater_id) + "','"+str(sprint_id) + "','"+str(sprint_name) + "')"
                            cursor.execute(insert_worklog)
                            row_count = row_count + 1
                    # st = 'total=' + str(total) + '; start_at=' + str(start_at) + '; row_count=' + str(
                    #     row_count)
                    # commonthread.write_log('DEBUG', 'worklog', st, True)
    except Exception as e:
        result = "error " + f"{e}"
        error = True
    result = result + '; row_count = ' + str(row_count)
    return error, result
