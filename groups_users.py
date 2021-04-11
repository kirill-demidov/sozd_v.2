import requests
import json
# import commonthread


def group_users(auth, connection):
    error = False
    result = "all finished OK"
    row_count = 0
    try:
        cursor = connection.cursor()
        url = "https://alterosmart.atlassian.net/rest/api/3/groups/picker?maxResults=1000"
        headers = {
           "Accept": "application/json"
        }

        response = requests.request(
           "GET",
           url,
           headers=headers,
           auth=auth
        )

        # getting groups and labels from jira
        data = json.loads(response.text)
        truncate_table = \
            'truncate table public.mrr_groups; truncate table public.mrr_labels;truncate table public.mrr_groups_users'
        cursor.execute(truncate_table)
        names = []
        sql_text = ''
        for n in range(0, len(data['groups'])):
            name = data['groups'][n]['name']
            names.append(name)
            html = data['groups'][n]['html']
            groupid = data['groups'][n]['groupId']
            insert_groups = \
                "insert into mrr_groups (name, html, groupid) values ("+"'"+name+"','"+html+"','"+groupid+"');"
            row_count = row_count + 1
            sql_text = sql_text + insert_groups
            # cursor.execute(insert_groups)

            for i in range(0, len(data['groups'][n]['labels'])):
                text = data['groups'][n]['labels'][i]['text']
                title = data['groups'][n]['labels'][i]['title']
                group_type = data['groups'][n]['labels'][i]['type']
                insert_labels = \
                    "insert into mrr_labels (label_text, title,lable_type, groupid) values (" +\
                    "'"+text+"','"+title+"','"+group_type+"','"+groupid+"');"
                row_count = row_count + 1
                sql_text = sql_text + insert_labels
                # cursor.execute(insert_labels)
            # st = 'groups=' + str(len(data['groups'])) + '; n=' + str(n) + '; row_count=' + str(row_count)
            # commonthread.write_log('DEBUG', 'groups_users', st, True)
        # getting user-group from jira
        cursor.execute(sql_text)

        sql_text = ''
        i = 0
        for name in names:
            url = "https://alterosmart.atlassian.net/rest/api/3/group/member?groupname="+name+"&maxResults=1000"
            headers = {"Accept": "application/json"}
            response_group = requests.request("GET",
                                              url,
                                              headers=headers,
                                              auth=auth
                                              )
            response_group = json.loads(response_group.text)
            for b in range(0, len(response_group['values'])):
                accountid = response_group['values'][b]['accountId']
                displayname = response_group['values'][b]['displayName']
                active = str(response_group['values'][b]['active'])
                account_type = response_group['values'][b]['accountType']
                groupname = name
                insert_group_users =\
                    "insert into mrr_groups_users (displayname, groupname,accounttype,accountid, isactive) values (" +\
                    "'"+displayname+"','"+groupname+"','"+account_type+"','"+accountid+"','"+active+"');"
        #         print(insert_group_users)
                row_count = row_count + 1
                sql_text = sql_text + insert_group_users
                # cursor.execute(insert_group_users)
            i = i + 1
            # st = 'names=' + str(len(names)) + '; n=' + str(i) + '; row_count=' + str(row_count)
            # commonthread.write_log('DEBUG', 'groups_users', st, True)
        cursor.execute(sql_text)
    except Exception as e:
        result = "error "+f"{e}"
        error = True
    result = result + '; row_count = ' + str(row_count)
    return error, result
