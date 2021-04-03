import requests
import json



def get_data(data, caption, default=''):
    if data:
        return data[caption]
    else:
        return default


def issues(auth,connection):
    error = False
    result = " all finished OK"
    issue_url = 'https://alterosmart.atlassian.net/browse/'
    startAt = 0
    try:
        cursor = connection.cursor()
        url = "https://alterosmart.atlassian.net/rest/api/3/search?jql="

        headers = {
            "Accept": "application/json"
        }

        needfinish = False
        truncate_table = 'truncate table public.mrr_issues;truncate table public.mrr_changelog;'
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
            for issue in issues['issues']:
                issue_id = issue['id']
                issue_link = issue_url + issue['key']
                issue_key = issue['key']
                issue_type = issue['fields']['issuetype']['name']
                issue_timespent = get_data(issue['fields'], 'timespent')
                project_id = issue['fields']['project']['id']
                issue_resolution = get_data(issue['fields']['resolution'], 'description', 'in progress')
                issue_created_date = issue['fields']['created']
                issue_updated_date = get_data(issue['fields'], 'updated')
                issue_priority = issue['fields']['priority']['name']
                issue_time_estimate = get_data(issue['fields'], 'timeestimate')
                issue_time_estimated_org = get_data(issue['fields'], 'timeoriginalestimate')
                issue_summ = get_data(issue['fields'], 'summary')
                issue_creator = get_data(issue['fields']['creator'], 'displayName')
                issue_reporter = get_data(issue['fields']['reporter'], 'displayName')
                issue_assignee = get_data(issue['fields']['assignee'], 'displayName')
                issue_status = get_data(issue['fields']['status'], 'name')
                insert_issues = "INSERT INTO public.mrr_issues(issue_id, issue_link, issue_key,\
                 issue_type, issue_timespent, project_id,issue_created_date,\
                 issue_updated_date, issue_priority, issue_time_estimate, issue_time_estimated_org,\
                  issue_creator, issue_reporter, issue_assignee, issue_status, issue_resolution, issue_summ)\
                 values(" + "'" + str(issue_id) + "','" + str(issue_link) + "','" + str(issue_key) + "','" + str(
                    issue_type) + "',\
                 '" + str(issue_timespent) + "','" + str(project_id) + "','" + str(issue_created_date) + "'\
                    ,'" + str(issue_updated_date) + "','" + str(issue_priority) + "','" + str(issue_time_estimate) + "',\
                    '" + str(issue_time_estimated_org) + "','" + str(issue_creator) + "','" + str(issue_reporter) + "'\
                    ,'" + str(issue_assignee) + "','" + str(issue_status) + "',\
                      '" + str(issue_resolution).replace("'", "''") + "','" + str(issue_summ).replace("'", "''") + "')"
                #                 print(insert_issues)
                cursor.execute(insert_issues)
                url_worklog = "https://alterosmart.atlassian.net/rest/api/3/issue/" + issue_id + "/changelog"
                headers = {
                    "Accept": "application/json"
                }

                response = requests.request(
                    "GET",
                    url_worklog,
                    headers=headers,
                    auth=auth
                )
                worklogs = json.loads(response.text)
                logs = worklogs['values']

                # print (type(items))
                for log in logs:
                    changelog_id = log['id']
                    items = log['items']
                    changelog_timestamp = log['created']
                    if "author" in log:
                        user_id = log['author']['displayName']
                    else:
                        user_id = 'Unknown'
                    for i in range(0, len(items)):
                        field_name = items[i]['field']
                        old_value = items[i]['fromString']
                        new_value = items[i]['toString']
                        insert_worklogs = "insert into public.mrr_changelog (changelog_id,issue_id,changelog_timestamp,\
                                            filed_name,old_value\
                                            ,new_value,user_id )\
                                            values(" + "'" + str(changelog_id) + "','" + str(issue_id) \
                                          + "','" + str(changelog_timestamp) + "','" + str(field_name) + "','" + str(
                            old_value).replace("'", "''") \
                                          + "','" + str(new_value).replace("'", "''") + "','" + str(user_id) + "')"
                        cursor.execute(insert_worklogs)
    except Exception as e:
        result = "error " + f"{e}"
        error = True
    return error, result

