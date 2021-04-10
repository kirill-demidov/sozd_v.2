import requests
import json


def project_roles_actors(auth, connection):
    result = "all finished OK"
    error = False
    row_count = 0
    sql_text = ''
    try:
        cursor = connection.cursor()
        url = "https://alterosmart.atlassian.net/rest/api/3/project/search"
        headers = {
           "Accept": "application/json"
        }

        response = requests.request(
           "GET",
           url,
           headers=headers,
           auth=auth
        )

        # getting projects from jira
        data = json.loads(response.text)
        truncate_table = 'truncate table public.mrr_projects;truncate table public.mrr_project_roles; ' \
                         'truncate table public.mrr_project_role_user'
        cursor.execute(truncate_table)
        project_ids = []
        for n in range(0, len(data['values'])):
            projectid = data['values'][n]['id']
            project_ids.append(projectid)
            is_private = data['values'][n]['isPrivate']
            key = data['values'][n]['key']
            name = data['values'][n]['name']
            project_type_key = data['values'][n]['projectTypeKey']
            insert_projects = \
                "insert into public.mrr_projects (projectid, isPrivate, key,name, projectTypeKey)\
                values ("+"'"+projectid+"','"+str(is_private)+"','"+key+"','"+name+"','"+project_type_key+"');"
            sql_text = sql_text + insert_projects
            # cursor.execute(insert_projects)
        cursor.execute(sql_text)
        for project in project_ids:
            sql_text = ''
            url = "https://alterosmart.atlassian.net/rest/api/3/project/"+project+"/roledetails"
        #     print(url)

            headers = {
               "Accept": "application/json"
                }

            project_roles = requests.request(
                   "GET",
                   url,
                   headers=headers,
                   auth=auth
                )
            data = json.loads(project_roles.text)
            for role in range(0, len(data)):
                role_id = data[role]['id']
                role_name = data[role]['name']
                insert_role_table = \
                    "insert into public.mrr_project_roles (roleid,rolename,projectid)values(" + "'" + str(role_id)\
                    + "','" + role_name + "','" + str(project) + "');"
                sql_text = sql_text + insert_role_table
                row_count = row_count + 1
                # cursor.execute(insert_role_table)
            cursor.execute(sql_text)
        #         print(role_id,role_name,project)

            sql_text = ''
            for n in data:
                url = 'https://alterosmart.atlassian.net/rest/api/3/project/'+project+'/role/'+str(n['id'])
        #         print(url)
                headers = {
                   "Accept": "application/json"
                }

                project_roles_actors = requests.request(
                   "GET",
                   url,
                   headers=headers,
                   auth=auth
                )
                roles_actors = json.loads(project_roles_actors.text)
                role_id = roles_actors['id']
        #         role_desc = roles_actors['description']
        #         print(roles_actors)
        #         break

                for i in range(0, len(roles_actors['actors'])):
                    actors = roles_actors['actors'][i]
                    user_name = actors['displayName']
                    insert_role_user_table = \
                        "insert into public.mrr_project_role_user (user_name,role_id,project_id)values(" + \
                        "'" + user_name + "','" + str(role_id) + "','" + str(project) + "');"
                    sql_text = sql_text + insert_role_user_table
                    row_count = row_count + 1
                    # cursor.execute(insert_role_user_table)
            cursor.execute(sql_text)
    except Exception as e:
        result = "error " + f"{e}"
        error = True
    result = result + '; row_count = ' + str(row_count)
    return error, result
