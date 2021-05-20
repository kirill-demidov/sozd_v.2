
txt_scripts = '''


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
