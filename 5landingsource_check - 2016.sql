/*
####################################################################################
# Name: check_landing_source (virality/ saving the url)
# Version:
#   2016/12/23 RS: Initial version
#   
####################################################################################
*/
 
 select c.source, c.landings, c.distinct_tpid_land, c.avg_land_pertpid, d.submits, d.distinct_tpid_submit, d.avg_submit_pertpid, round(d.distinct_tpid_submit/c.distinct_tpid_land*100,2) as submit2land_percnt from 
 
 (select source, sum(landing_flg) as landings, count(distinct tapad_id) as distinct_tpid_land, round(sum(landing_flg)/count(distinct tapad_id),2) as avg_land_pertpid from 
(select tapad_id, 
 case when action_id ='undefined' then 1 else 0 end as landing_flg,
 case when action_id like '%submit%' then 1 else 0 end as submit_flg,
	case
		when referrer_url like '%kaidee%' then 'kd'
		when referrer_url like '%facebook%' then 'fb' else 'other' end as source   
from 
( select 
b.value as tapad_id, a.header.referrer_url as referrer_url, a.action_id as action_id 
 from default.tracked_events a, a.header.incoming_ids b where a.property_id = '2868' and (a.action_id ='undefined') and lcase(a.header.referrer_url) like '%j5%') A) B group by source) C join 
 
 
 ( select source, sum(submit_flg) as submits, count(distinct tapad_id) as distinct_tpid_submit, round(sum(submit_flg)/count(distinct tapad_id),2) as avg_submit_pertpid from 
(select tapad_id, 
 case when action_id ='undefined' then 1 else 0 end as landing_flg,
 case when action_id like '%submit%' then 1 else 0 end as submit_flg,
	case
		when referrer_url like '%kaidee%' then 'kd'
		when referrer_url like '%facebook%' then 'fb' else 'other' end as source   
from 
( select 
b.value as tapad_id, a.header.referrer_url as referrer_url, a.action_id as action_id 
 from default.tracked_events a, a.header.incoming_ids b where a.property_id = '2868' and (a.action_id like '%submit%') and lcase(a.header.referrer_url) like '%j5%') A) B group by source) D on c.source = d.source;
 
 
 