import requests
import json

def group_users(auth, connection):
    error = False
    result = " all finished OK"
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

        ##### getting groups and labels from jira
        data = json.loads(response.text)
        truncate_table = 'truncate table public.mrr_groups; truncate table public.mrr_labels;truncate table public.mrr_groups_users'
        cursor.execute(truncate_table)
        names = []
        for n in range(0,len(data['groups'])):
            name = data['groups'][n]['name']
            names.append(name)
            html = data['groups'][n]['html']
            groupid = data['groups'][n]['groupId']
            insert_groups = "insert into mrr_groups (name, html, groupid) values ("+"'"+name+"','"+html+"','"+groupid+"')"
            cursor.execute(insert_groups)

            for i in range (0, len(data['groups'][n]['labels'])):
                text = data['groups'][n]['labels'][i]['text']
                title = data['groups'][n]['labels'][i]['title']
                type = data['groups'][n]['labels'][i]['type']
                insert_labels = "insert into mrr_labels (label_text, title,lable_type, groupid) values ("+"'"+text+"','"+title+"','"+type+"','"+groupid+"')"
                cursor.execute(insert_labels)
        #####getting user-group from jira

        for name in names:
            url = "https://alterosmart.atlassian.net/rest/api/3/group/member?groupname="+name+"&maxResults=1000"
            headers = {
           "Accept": "application/json"
            }
            response_group = requests.request(
            "GET",
            url,
            headers=headers,
            auth=auth
            )
            response_group = json.loads(response_group.text)
            for b in range(0,len(response_group['values'])):
                accountid = response_group['values'][b]['accountId']
                displayname = response_group['values'][b]['displayName']
                active = str(response_group['values'][b]['active'])
                accountType = response_group['values'][b]['accountType']
                groupname = name
                insert_group_users ="insert into mrr_groups_users (displayname, groupname,accounttype,accountid, isactive) values ("+"'"+displayname+"','"+groupname+"','"+accountType+"','"+accountid+"','"+active+"')"
        #         print(insert_group_users)
                cursor.execute(insert_group_users)
    except Exception as e:
        result = "error "+f"{e}"
        error = True
    return error,result
