

-- 

with cte as (select law_id from mrr_laws ml where law_introductiondate between '2020-01-03' and '2021-06-17')

SELECT count(*), b.comittee_name 
into temporary table a_2020_2021
FROM public.mrr_bridge_profile_comittees_laws a join mrr_committees b 
												on a.profile_committee_id =b.id 
												join cte on a.law_id = cte.law_id
group by 
b.comittee_name



with cte as (select law_id from mrr_laws ml where law_introductiondate between '2018-01-03' and '2019-06-17')

SELECT count(*), b.comittee_name 
into temporary table a_2018_2019
FROM public.mrr_bridge_profile_comittees_laws a join mrr_committees b 
												on a.profile_committee_id =b.id 
												join cte on a.law_id = cte.law_id
group by 
b.comittee_name


with cte as (select law_id from mrr_laws ml where law_introductiondate between '2015-01-03' and '2016-06-17')

SELECT count(*), b.comittee_name 
into temporary table a_2015_2016
FROM public.mrr_bridge_profile_comittees_laws a join mrr_committees b 
												on a.profile_committee_id =b.id 
												join cte on a.law_id = cte.law_id
group by 
b.comittee_name

select a.count data_2021 , b.count data_2019 , c.count data_2016 , a.comittee_name
from a_2020_2021 a left join  a_2018_2019 b on a.comittee_name=b.comittee_name
				   left join a_2015_2016 c on a.comittee_name=c.comittee_name

select * 
from mrr_legistation_stages mls 
order by cast(phase_id as int)
				   
select * 
from mrr_laws ml 


-- ?????? ????? 
with mrr_subject as (
select id, department_name, department_name as department
from mrr_federal_departmens  
union 
select id, department_name, '???????????? ???????????'
from mrr_regional_departmens mrd
union 
select deputy_id , deputy_name , deputy_position
from (
select  deputy_id , deputy_name , deputy_position , max(fraction_startdate)
from mrr_deputy_by_fraction mdbf 
group by  deputy_id , deputy_name , deputy_position) as a
),
laws as (

select a.department_id  as subject_id,a.law_id , mrr_subject.department
from mrr_bridge_department_laws a  join mrr_subject  
									on a.department_id =mrr_subject.id
	     							where a.law_id in (select b.law_id  from mrr_kovid a join 
													mrr_laws b on a.law_number=b.law_number)
union 
select deputy_id, law_id , mbld.deputy_position 
from mrr_bridge_laws_deputy mbld 
where mbld.law_id in (select b.law_id  from mrr_kovid a join 
													mrr_laws b on a.law_number=b.law_number)
													
)
select distinct b.law_id ,count(*) over ( partition by law_name, department), department
into temp table a
from laws a join mrr_laws b 
on a.law_id = b.law_id 
order by 1 


select count(*), law_id 
into temp table multi_law
from a
group by law_id 
having count (*)>1


select *
from multi_law

select *
from a
where law_id in (select law_id from multi_law)


select *
from a
where law_id in (select law_id from single_law)



