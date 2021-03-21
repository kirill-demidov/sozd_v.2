import requests
from requests.auth import HTTPBasicAuth
import json
import psycopg2


def project_roles_actors():
    result = "all finished OK"
    try:
        connection = psycopg2.connect(user = "postgres",
                                          password = "password",
                                          host = "127.0.0.1",
                                          port = "5432",
                                          database = 'postgres')
        connection.autocommit=True
        cursor = connection.cursor()
        url = "https://alterosmart.atlassian.net/rest/api/3/project/search"

        auth = HTTPBasicAuth("k.demidov@alterosmart.com", "L99Ib8xsuFJKtvTn8SpM8F3C")

        headers = {
           "Accept": "application/json"
        }

        response = requests.request(
           "GET",
           url,
           headers=headers,
           auth=auth
        )

        ##### getting projects from jira
        data = json.loads(response.text)
        truncate_table ='truncate table public.mrr_projects;truncate table public.mrr_project_roles; truncate table public.mrr_project_role_user'
        cursor.execute(truncate_table)
        project_ids = []
        for n in range(0,len(data['values'])):
            projectid = data['values'][n]['id']
            project_ids.append(projectid)
            isPrivate = data['values'][n]['isPrivate']
            key = data['values'][n]['key']
            name = data['values'][n]['name']
            projectTypeKey = data['values'][n]['projectTypeKey']
            insert_projects = "insert into public.mrr_projects (projectid, isPrivate, key,name, projectTypeKey)\
                                values ("+"'"+projectid+"','"+str(isPrivate)+"','"+key+"','"+name+"','"+projectTypeKey+"')"
            cursor.execute(insert_projects)
        for project in project_ids:
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
            for role in range(0,len(data)):
                role_id = data[role]['id']
                role_name = data[role]['name']
                insert_role_table = "insert into public.mrr_project_roles (roleid,rolename,projectid)values("+"'"+str(role_id)+"','"+role_name+"','"+str(project)+"')"
                cursor.execute(insert_role_table)
        #         print(role_id,role_name,project)

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
                roles_actors=json.loads(project_roles_actors.text)
                role_id = roles_actors['id']
        #         role_desc = roles_actors['description']
        #         print(roles_actors)
        #         break

                for i in range(0,len(roles_actors['actors'])):
                    actors=roles_actors['actors'][i]
                    user_name = actors['displayName']
                    insert_role_user_table = "insert into public.mrr_project_role_user (user_name,role_id,project_id)values("+"'"+user_name+"','"+str(role_id)+"','"+str(project)+"')"
                    cursor.execute(insert_role_user_table)
    except Exception as e:
        result = "error "+f"{e}"
    return result
st = project_roles_actors()
print(st)
