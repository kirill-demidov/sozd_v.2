
txt_scripts = '''
CREATE TABLE if not exists public.dwh_bridge_user_group (
				user_id varchar NOT NULL,
				group_id varchar NOT NULL,
				id int4 NOT NULL,
				created_at timestamp default CURRENT_TIMESTAMP,
				CONSTRAINT dwh_bridge_user_group_pk PRIMARY KEY (id)
);



CREATE TABLE  if not exists public.dwh_bridge_user_role_project (
	id int4 NOT NULL,
	user_id varchar NULL,
	project_id int4 NULL,
	role_id int4 NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT dwh_bridge_user_role_project_pk PRIMARY KEY (id)
);


CREATE TABLE  if not exists public.dwh_dim_groups (
	group_id varchar NOT NULL,
	group_name varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT dwh_dim_groups_pk PRIMARY KEY (group_id)
);



CREATE TABLE if not exists  public.dwh_dim_projects (
	project_id int4 NOT NULL,
	project_key varchar NULL,
	project_name varchar NULL,
	project_type varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT dwh_dim_projects_pk PRIMARY KEY (project_id)
);



CREATE TABLE  if not exists public.dwh_dim_roles (
	role_id int4 NOT NULL,
	role_name varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT dwh_dim_roles_pk PRIMARY KEY (role_id)
);


CREATE TABLE  if not exists public.dwh_dim_users (
	account_type varchar NULL,
	user_name varchar NULL,
	user_status varchar NULL,
	user_id varchar NOT NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT dwh_dim_users_pk PRIMARY KEY (user_id)
);


CREATE TABLE if not exists  public.dwh_fact_issue (
	issue_id int4 NOT NULL,
	issue_link varchar NULL,
	issue_type varchar NULL,
	project_id int4 NULL,
	issue_created_date date NULL,
	issue_updated_date date NULL,
	issue_priority varchar NULL,
	issue_time_estimate int4 NULL,
	issue_time_estimate_org int4 NULL,
	reporter varchar NULL,
	creator varchar NULL,
	assignee varchar NULL,
	issue_status varchar NULL,
	issue_summary varchar NULL,
	issue_resolution varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT dwh_fact_issue_pk PRIMARY KEY (issue_id)
);


CREATE TABLE  if not exists public.dwh_fact_issues_worklog (
	worklog_id int4 NOT NULL,
	issue_id int4 NULL,
	project_id varchar NULL,
	update_date timestamp NULL,
	time_spent_sec int4 NULL,
	updater_id varchar NULL,
	active_sprint_id int4 NULL,
	active_sprint_name varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT dwh_fact_issues_worklog_pk PRIMARY KEY (worklog_id)
);



CREATE TABLE  if not exists public.dwh_fact_status_duration (
	changelog_id int4 NOT NULL,
	issue_id int4 NULL,
	field_name varchar NULL,
	status varchar NULL,
	duration_hours int4 NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT dwh_fact_status_duration_pk PRIMARY KEY (changelog_id)
);

CREATE TABLE if not exists public.mng_bridge_user_group (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	user_id varchar NULL,
	group_id varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);

CREATE TABLE  if not exists public.mng_bridge_user_role_project (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	user_id varchar NULL,
	project_id int4 NULL,
	role_id int4 NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);


CREATE TABLE  if not exists public.mrr_changelog (
	changelog_id varchar NULL,
	issue_id varchar NULL,
	changelog_timestamp varchar NULL,
	filed_name varchar NULL,
	old_value varchar NULL,
	new_value varchar NULL,
	user_id varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);


CREATE TABLE  if not exists public.mrr_groups (
	"name" varchar NULL,
	html varchar NULL,
	groupid varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);


CREATE TABLE  if not exists public.mrr_groups_users (
	displayname varchar NULL,
	groupname varchar NULL,
	accounttype varchar NULL,
	accountid varchar NULL,
	isactive varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);


CREATE TABLE  if not exists public.mrr_issue_fields (
	field_id varchar NULL,
	field_name varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);



CREATE TABLE  if not exists public.mrr_issues (
	issue_id varchar NULL,
	issue_link varchar NULL,
	issue_key varchar NULL,
	issue_type varchar NULL,
	issue_timespent varchar NULL,
	project_id varchar NULL,
	issue_resolution varchar NULL,
	issue_created_date varchar NULL,
	issue_updated_date varchar NULL,
	issue_priority varchar NULL,
	issue_time_estimate varchar NULL,
	issue_time_estimated_org varchar NULL,
	issue_summ varchar NULL,
	issue_creator varchar NULL,
	issue_reporter varchar NULL,
	issue_assignee varchar NULL,
	issue_status varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);



CREATE TABLE  if not exists public.mrr_labels (
	label_text varchar NULL,
	title varchar NULL,
	lable_type varchar NULL,
	groupid varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);


CREATE TABLE  if not exists public.mrr_project_role_user (
	user_name varchar NULL,
	role_id int4 NULL,
	project_id int4 NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);


CREATE TABLE  if not exists public.mrr_project_roles (
	roleid int4 NULL,
	rolename varchar NULL,
	projectid int4 NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);

CREATE TABLE  if not exists public.mrr_projects (
	projectid int4 NULL,
	isprivate varchar(100) NULL,
	"key" varchar NULL,
	"name" varchar NULL,
	projecttypekey varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);


CREATE TABLE  if not exists public.mrr_users (
	accounttype varchar NULL,
	display_name varchar NULL,
	status varchar NULL,
	userid varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);



CREATE TABLE  if not exists public.mrr_worklog (
	worklog_id int4 NULL,
	issue_id int4 NULL,
	create_date varchar NULL,
	update_date varchar NULL,
	start_date varchar NULL,
	time_spent varchar NULL,
	time_spent_sec int4 NULL,
	updater_id varchar NULL,
	active_sprint_id int4 NULL,
	active_sprint_name varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);


CREATE TABLE  if not exists public.stg_bridge_user_group (
	user_id varchar NOT NULL,
	group_id varchar NOT NULL,
	id int4 NOT NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT stg_bridge_user_group_pk PRIMARY KEY (id)
);


CREATE TABLE  if not exists public.stg_bridge_user_role_project (
	id int4 NOT NULL,
	user_id varchar NULL,
	project_id int4 NULL,
	role_id int4 NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT stg_bridge_user_role_project_pk PRIMARY KEY (id)
);


CREATE TABLE  if not exists public.stg_changelog_duration (
	changelog_id int4 NULL,
	field_name varchar NULL,
	value varchar NULL,
	duration_hours int4 NULL,
	issue_id int4 NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);

CREATE TABLE  if not exists public.stg_dim_groups (
	group_id varchar NOT NULL,
	group_name varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT stg_dim_groups_pk PRIMARY KEY (group_id)
);


CREATE TABLE  if not exists public.stg_dim_projects (
	project_id int4 NOT NULL,
	project_key varchar NULL,
	project_name varchar NULL,
	project_type varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT stg_dim_projects_pk PRIMARY KEY (project_id)
);


CREATE TABLE  if not exists public.stg_dim_roles (
	role_id int4 NOT NULL,
	role_name varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT stg_dim_roles_pk PRIMARY KEY (role_id)
);

CREATE TABLE  if not exists public.stg_dim_user_group (
	user_name varchar NULL,
	account_type varchar NULL,
	group_name varchar NULL,
	user_status varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);

CREATE TABLE  if not exists public.stg_dim_user_project_role (
	user_name varchar NULL,
	project_name varchar NULL,
	role_name varchar NULL,
	user_account_type varchar NULL,
	user_status varchar NULL,
	group_name varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP
);


CREATE TABLE  if not exists public.stg_dim_users (
	account_type varchar NULL,
	user_name varchar NULL,
	user_status varchar NULL,
	user_id varchar NOT NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT stg_dim_users_pk PRIMARY KEY (user_id)
);

CREATE TABLE  if not exists public.stg_fact_issue (
	issue_id int4 NOT NULL,
	issue_link varchar NULL,
	issue_type varchar NULL,
	project_id int4 NULL,
	issue_created_date date NULL,
	issue_updated_date date NULL,
	issue_priority varchar NULL,
	issue_time_estimate int4 NULL,
	issue_time_estimate_org int4 NULL,
	reporter varchar NULL,
	creator varchar NULL,
	assignee varchar NULL,
	issue_status varchar NULL,
	issue_summary varchar NULL,
	issue_resolution varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT stg_fact_issue_pk PRIMARY KEY (issue_id)
);


CREATE TABLE  if not exists public.stg_fact_issues_worklog (
	worklog_id int4 NOT NULL,
	issue_id int4 NULL,
	project_id varchar NULL,
	update_date timestamp NULL,
	time_spent_sec int4 NULL,
	updater_id varchar NULL,
	active_sprint_id int4 NULL,
	active_sprint_name varchar NULL,
	created_at timestamp default CURRENT_TIMESTAMP,
	CONSTRAINT stg_fact_issues_worklog_pk PRIMARY KEY (worklog_id)
);


CREATE TABLE  if not exists public.stg_fact_status_duration (
	changelog_id int4 NOT NULL,
	issue_id int4 NULL,
	field_name varchar NULL,
	status varchar NULL,
	duration_hours int4 NULL,
	CONSTRAINT stg_fact_status_duration_pk PRIMARY KEY (changelog_id)
);

CREATE OR REPLACE FUNCTION public.delpole(txt text, fdelimiter text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
  n integer;
  TxtResult text;
begin
  n:=position(fdelimiter in txt);
  if (n<>0) then TxtResult:=right(txt, Length(Txt)-n-length(fdelimiter));  else	TxtResult:=''; end if;
  return TxtResult;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.get_text_merge(schemaetalon text, tableetalon text, schematarget text, tabletarget text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
	declare
	ColumnsName text;
	ColumnsPKName text;
	TextWhere text;
    TextUpdate text;
    NameKey text;
    fDelimiter text;
    Rab text;
begin
	ColumnsName:=(select public.getallcolumnsname(schemaEtalon, tableEtalon));
	ColumnsPKName:=(select public.getPKcolumnsname(schemaEtalon, tableEtalon));
    TextWhere:=(select public.getTextWhere(ColumnsPKName, schemaTarget, tableTarget));
    Rab:=ColumnsName;
    TextUpdate:='';
    fDelimiter:=',';
   while (Rab<>'')  loop
	  NameKey:=(select public.GetPole(Rab, fdelimiter)); -- имя поля
	  Rab:=(select public.DelPole(Rab, fdelimiter));
      if (position(NameKey in ColumnsPKName)=0) then
      begin
        if (TextUpdate<>'') then TextUpdate:=TextUpdate || ',';  end if;
 	    TextUpdate:=TextUpdate || ' ' || NameKey || '= (select ' || NameKey || ' from ' || schemaEtalon || '.' || TableEtalon || ' ' || TextWhere || ')';
 	  end;
      end if;
	end loop;
    return format('insert into %s.%s (%s)	select %s from %s.%s  on conflict (%s) do update set %s;',
		SchemaTarget, TableTarget, ColumnsName, ColumnsName, schemaEtalon, tableEtalon, columnsPKName, TextUpdate);

end;
$function$
;
CREATE OR REPLACE FUNCTION public.stg_populate_duration_changelog_table()
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
  ftime_start timestamp;
  ftime_start_prev timestamp;
  fissue_id text;
  fissue_id_prev text;
  fold_value text;
  duration int;
  fchangelog_id text;
  curs cursor for select distinct changelog_id, issue_id, changelog_timestamp, old_value from mrr_changelog  where filed_name = 'status' order by issue_id,changelog_timestamp;
begin
	truncate table stg_changelog_duration;
	fissue_id_prev:=-1;
	open curs;
	loop
		fetch curs into fchangelog_id, fissue_id, ftime_start, fold_value;
		if not found then exit; end if;
		if fissue_id_prev <> fissue_id then 
			ftime_start_prev := (select issue_created_date from mrr_issues where issue_id = fissue_id);
		end if; --new issue_id
		duration :=  EXTRACT(HOUR FROM ftime_start-ftime_start_prev)+extract(day from ftime_start-ftime_start_prev)*24 ;
		insert into stg_changelog_duration (changelog_id , field_name , value , duration_hours , issue_id ) values (fchangelog_id::int, 'status', fold_value, duration, fissue_id::int);
	end loop;
	close curs;
	return 'Finished';
end;
$function$
;

CREATE OR REPLACE FUNCTION public.getallcolumnsname(schemaname text, tablename text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
	OneName text;
	ResultText text;
	Spisok cursor for select column_name from information_schema."columns" c  where table_name=tablename and table_schema =SchemaName;
begin
    resultText:='';
	open Spisok;
	loop
  	  fetch Spisok into onename ;
  	  if (not found) then exit; end if;
	  if resultText<>'' then resultText:=resultText || ', '; end if;
	  resultText:=resultText || oneName;
  	end loop;
  	close Spisok;
    return resultText;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.getpkcolumnsname(schemaname text, tablename text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
	OneName text;
	ResultText text;
	Spisok cursor for
		select kc.column_name
		from information_schema.table_constraints tc,
		  information_schema.key_column_usage kc
		where
			 tc.table_name =kc.table_name  and
			 tc.table_schema =kc.table_schema and
			 kc.constraint_name=tc.constraint_name and
  			 tc.table_schema=SchemaName and tc.table_name=TableName and constraint_type='PRIMARY KEY';
begin
    resultText:='';
	open Spisok;
	loop
  	  fetch Spisok into onename ;
  	  if (not found) then exit; end if;
	  if resultText<>'' then resultText:=resultText || ', '; end if;
	  resultText:=resultText || oneName;
  	end loop;
  	close Spisok;
    return resultText;
END;
$function$
;

CREATE OR REPLACE FUNCTION public.getpole(txt text, fdelimiter text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
  n integer;
  pole text;
begin
  n:=position(fdelimiter in txt);
  if (n<>0) then pole:=left(txt,n-1);  else Pole:=Txt;  end if;
  return pole;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.gettextwhere(columnspkname text, schematarget text, tabletarget text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
declare
  n integer;
  NameKey text;
  TextWhere text;
  Fdelimiter text;
 begin
	TextWhere:='';
	fDelimiter:=',';
	while (columnsPKName<>'')  loop
	  NameKey:=(select public.GetPole(columnsPKName, fdelimiter));
	  columnsPKName:=(select public.DelPole(columnsPKName, fdelimiter));
	  if (TextWhere<>'') then TextWhere:=TextWhere || ' and '; end if;
	  TextWhere:=TextWhere || schematarget || '.' || tabletarget || '.' || NameKey || '=' || NameKey;
	end loop;
    return 'where ' || TextWhere;
 end;
 $function$
;

CREATE OR REPLACE PROCEDURE public.insert_mng_bridge_user_group()
 LANGUAGE sql
AS $procedure$
insert into mng_bridge_user_group (user_id, group_id)
select a.accountid user_id , b.groupid group_id
from mrr_groups_users a join mrr_groups  b on a.groupname =b.name
except
select user_id, group_id
from mng_bridge_user_group
 $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_mng_user_role_project()
 LANGUAGE sql
AS $procedure$
insert into  mng_bridge_user_role_project (user_id, project_id,role_id)
select   b.userid,project_id ,role_id
from mrr_project_role_user a join mrr_users b on a.user_name =b.display_name
except
select user_id,project_id , role_id
from mng_bridge_user_role_project
ON conflict do nothing;
 $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_stg_bridge_user_group()
 LANGUAGE sql
AS $procedure$
truncate table public.stg_bridge_user_group;
insert into stg_bridge_user_group (user_id, group_id, id)
select  a.accountid  , b.groupid, c.id
from mrr_groups_users a join mrr_groups b on a.groupname =b."name"
left join mng_bridge_user_group c on a.accountid =c.user_id and b.groupid =c.group_id
ON conflict do nothing;
 $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_stg_dim_groups()
 LANGUAGE sql
AS $procedure$
truncate table public.stg_dim_groups;
insert into public.stg_dim_groups (group_id, group_name )
select distinct groupid, name
from public.mrr_groups
    $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_stg_dim_projects()
 LANGUAGE sql
AS $procedure$
truncate table stg_dim_projects;
insert into stg_dim_projects (project_id , project_key , project_name ,project_type )
select distinct cast(projectid as int), key, name, projecttypekey
from public.mrr_projects
    $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_stg_dim_roles()
 LANGUAGE sql
AS $procedure$
truncate table public.stg_dim_roles;
insert into public.stg_dim_roles (role_id,role_name  )
select distinct roleid,rolename
from public.mrr_project_roles
    $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_stg_dim_user_group()
 LANGUAGE sql
AS $procedure$
truncate table stg_dim_user_group;
insert into stg_dim_user_group
select
a.display_name user_name,
a.accounttype account_type,
b.groupname group_name,
case when a.status ='True' then 'User Is Active' else 'User Is Not Active' end user_status
from mrr_users a left join mrr_groups_users b on a.display_name =b.displayname;
    $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_stg_dim_user_project_role()
 LANGUAGE sql
AS $procedure$
truncate table stg_dim_user_project_role;

with cte as (
select
case when b.displayname is  null then a.user_name else b.displayname  end user_name,
case when b.groupname is null then 'User added personaly' else b.groupname end group_name,
mp."name" as project_name,
c.rolename
from  mrr_project_role_user a left join mrr_groups_users b on a.user_name =b.groupname
							  left join mrr_projects mp on a.project_id =mp.projectid
							  left join mrr_project_roles c on a.project_id =c.projectid  and a.role_id =c.roleid
							)
insert into  stg_dim_user_project_role (user_name, project_name, role_name, user_account_type,
user_status, group_name)
select
a.user_name,
a.project_name,
a.rolename,
b.accounttype,
b.status,
a.group_name
from cte a join mrr_users b on a.user_name=b.display_name  ;
    $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_stg_dim_users()
 LANGUAGE sql
AS $procedure$
truncate table stg_dim_users;
insert into stg_dim_users (account_type, user_name, user_status, user_id)
select accounttype ,display_name,
case when status='True' then 'Active' else 'Disabled' end ,userid
from mrr_users
    $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_stg_fact_issue()
 LANGUAGE sql
AS $procedure$
truncate table public.stg_fact_issue;
insert into  public.stg_fact_issue (issue_id,
									issue_link,
									issue_type,
									project_id,
									issue_created_date,
									issue_updated_date,
									issue_priority,
									issue_time_estimate,
									issue_time_estimate_org,
									reporter,
									creator,
									assignee,
									issue_status,
									issue_summary,
									issue_resolution)
select
	cast(issue_id as int)issue_id ,
	issue_link ,
	issue_type ,
	cast(project_id as int)project_id ,
	cast(issue_created_date as date)issue_created_date,
	cast(issue_updated_date as date)issue_updated_date,
	issue_priority,
	cast(replace(issue_time_estimate, 'None', '0')as int)issue_time_estimate,
	cast(replace(a.issue_time_estimated_org , 'None', '0')as int)issue_time_estimate_org,
	reporter.userid as reporter,
	creator.userid as creator,
	assignee.userid as assignee,
	a.issue_status,
	a.issue_summ ,
	a.issue_resolution

from
	mrr_issues a join mrr_users reporter  on a.issue_reporter = reporter.display_name
				 join mrr_users creator on a.issue_creator = creator.display_name
				 join mrr_users assignee on a.issue_creator = assignee.display_name

ON conflict do nothing;
 $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_stg_fact_issues_worklog()
 LANGUAGE sql
AS $procedure$
truncate table public.stg_fact_issues_worklog;
insert into public.stg_fact_issues_worklog
select distinct
	a.worklog_id ,
	a.issue_id ,
	b.project_id,
	cast(update_date as timestamp)as update_date ,
	a.time_spent_sec,
	a.updater_id,
	a.active_sprint_id,
	a.active_sprint_name
from
	mrr_worklog a
join mrr_issues b on
	cast(a.issue_id as int) = cast(b.issue_id as int)

    $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_stg_fact_status_duration()
 LANGUAGE sql
AS $procedure$
truncate table stg_fact_status_duration;
insert into  stg_fact_status_duration (changelog_id,issue_id, field_name, status, duration_hours)
select a.changelog_id::int,a.issue_id::int , a.filed_name , b.value , b.duration_hours
from mrr_changelog a join stg_changelog_duration b on a.changelog_id::int =b.changelog_id
where a.filed_name = 'status'
ON conflict do nothing;
 $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_stg_user_role_project()
 LANGUAGE sql
AS $procedure$
truncate table public.stg_bridge_user_role_project;
insert into  public.stg_bridge_user_role_project (user_id, project_id,role_id, id)
select   b.userid,a.project_id ,a.role_id , c.id
from mrr_project_role_user a join mrr_users b on a.user_name =b.display_name
							 join mng_bridge_user_role_project c on a.project_id =c.project_id
							 									and a.role_id =c.role_id
							 									and b.userid =c.user_id
ON conflict do nothing;
 $procedure$
;

CREATE OR REPLACE PROCEDURE public.merge_table(schemaetalon text, tableetalon text, schematarget text, tabletarget text)
 LANGUAGE plpgsql
AS $procedure$
	declare
    Rab text;
begin
    Rab:=(select public.get_text_merge(schemaetalon, tableetalon, schematarget, tabletarget));
	execute format(Rab);

end;
$procedure$
;'''
