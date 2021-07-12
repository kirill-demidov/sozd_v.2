

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


