def postgres_etl(connection):
    try:
        error = False
        result = " all finished OK"
        cursor = connection.cursor()
        cursor.execute(
            """
                 call public.insert_mng_bridge_user_group() ;
                 call public.insert_mng_user_role_project();
                 call public.insert_stg_bridge_user_group();
                 call public.insert_stg_dim_groups();
                 call public.insert_stg_dim_projects();
                 call public.insert_stg_dim_roles();
                 call public.insert_stg_dim_user_group();
                 call public.insert_stg_dim_user_project_role();
                 call public.insert_stg_dim_users();
                 -- select public.stg_populate_duration_changelog_table();
                 call public.insert_stg_fact_issue();
                 call public.insert_stg_fact_issues_worklog();
                 call public.merge_table ('public','stg_bridge_user_group','public','dwh_bridge_user_group' );
                 call public.merge_table ('public','stg_bridge_user_role_project','public',
                 'dwh_bridge_user_role_project' );
                 call public.merge_table ('public','stg_dim_groups','public','dwh_dim_groups' );
                 call public.merge_table ('public','stg_dim_projects','public','dwh_dim_projects' );
                 call public.merge_table ('public','stg_dim_roles','public','dwh_dim_roles' );
                 --call public.merge_table ('public','stg_fact_status_duration','public','dwh_fact_status_duration' );
                 call public.merge_table ('public','stg_dim_users','public','dwh_dim_users' );
                 call public.merge_table ('public','stg_fact_issue','public','dwh_fact_issue' );
                 call public.merge_table ('public','stg_fact_issues_worklog','public','dwh_fact_issues_worklog' );
            """
            )
    except Exception as e:
        result = "error " + f"{e}"
        error = True
    return error, result
