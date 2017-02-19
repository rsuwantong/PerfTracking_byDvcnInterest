/*
####################################################################################
# Name: meas_table
# Description: Create an Impala table of measurement data
# Input: taps & tracked_events
# Version:
#   2016/11/25 RS: Initial version
#   2016/11/28 RS: Add room name from the header_url
####################################################################################
*/


drop table if exists meas_ana.meas_table_auto;

create table meas_ana.meas_table_auto
(sight_date STRING, tapad_id STRING, hl_platform STRING, dvc_techname STRING, carrier STRING, offer STRING, source STRING, room_id INT, ful_channel STRING, 
imps INT, clicks INT, selects INT, landings INT, submits INT ) partitioned by (year INT, month INT, day INT) stored as PARQUET;

insert into table meas_ana.meas_table_auto partition(year=${var:year}, month=${var:month}, day=${var:day}) 

select case when c.sight_date is not null then c.sight_date else f.sight_date end as sight_date, 
		case when c.tapad_id is not null then c.tapad_id else f.tapad_id end as tapad_id, 
		case when c.carrier is not null then c.carrier else f.carrier end as carrier, 
		case when c.offer is not null then c.offer else f.offer end as offer, 
		case when c.source is not null then c.source else f.source end as source, 
		c.room_id, c.room_name, f.ful_channel, c.imps, c.clicks, f.landings, f.selects, f.submits, 
		case when c.year is not null then c.year else f.year end as year,
		case when c.month is not null then c.month else f.month end as month,
		case when c.day is not null then c.day else f.day end as day 
		from 
 (select sight_date, tapad_id, carrier, offer,  'kd' as source, room_id, room_name,  
sum(imp_flg) as imps, sum(click_flg) as clicks, year, month, day   from 
(select sight_date, tapad_id, 
	case when action_id ='impression' then 1 else 0 end as imp_flg, 
    case when action_id ='click' then 1 else 0 end as click_flg, 		
	case when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier, 
	offer, room_id,  
	case 
		when regexp_replace(regexp_replace(regexp_replace(header_url,'.*kaidee\\.com/',''),'/.*',''),'\\?.*','') like 'c%' and header_url not like 'categories' then regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(header_url,'.*kaidee\\.com/',''),'/.*',''),'\\?.*',''),'.*[0-9]-',''),'-.*','') end as room_name,
		year, month, day 
	from 
(select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sight_date, b.value as tapad_id, a.action_id as action_id, cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 + cast(split_part(a.header.ip_address,'.',2) as INT)*65536 + cast(split_part(a.header.ip_address,'.',3) as INT)*256+ cast(split_part(a.header.ip_address,'.',4) as INT) ip_number, 
	case
		when a.tactic_id = 186858 then 'mnp-device-discount-samsung'
		when a.tactic_id = 191242 then 'mnp-device-discount-samsung'
		when a.tactic_id = 191243 then 'mnp-free-device'
		when a.tactic_id = 191244  then 'tariff'
		when a.tactic_id = 199183 then 'booster'
		when a.tactic_id in (197236, 213768) then 'mnp-samsung-galaxy-j2'
		when a.tactic_id = 200320 then 'mnp-samsung-galaxy-j5'
		when a.tactic_id = 201014 then 'mnp-samsung-galaxy-a5'
		when a.tactic_id in (203164,217118) then 'mnp-asus-zenfone-45' 
		when a.tactic_id = 214301 then 'mnp-oppo-mirror5' 
		when a.tactic_id = 217958 then 'mnp-free-dtac-pocket-wifi' 
		when a.tactic_id = 221701 then 'mnp-free-dtac-phone-s2'		end as offer, 
	cast(q.value as int) as room_id, h.value as header_url , a.year, a.month, a.day
 from default.taps a, a.header.incoming_ids b, a.header.query_params q, a.header.http_headers h where q.key='ext_cat' and h.key = 'Referer' and a.campaign_id=5138 and a.action_id IN ('impression','click') and a.year=${var:year} and a.month=${var:month} and a.day =${var:day} ) A) B group by sight_date, tapad_id, carrier, offer, source, room_id, room_name) C 
 
 full outer join 
 
 (select  sight_date, tapad_id, carrier, offer,  source, ful_channel,  
		sum(select_flg) as selects, sum(landing_flg) as landings, sum(submit_flg) as submits, year, month, day  from 
 
(select sight_date, 
	   tapad_id, 
	   case when action_id ='undefined' then 1 else 0 end as landing_flg, 
	   case when action_id like '%select%' then 1 else 0 end as select_flg, 
       case when action_id like '%submit%' then 1 else 0 end as submit_flg, 		
	case 
		when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier,
	case 
		when referrer_url like '%special-package%' then 'tariff' 
		when referrer_url like '%asus-zenfone-45%' then 'mnp-asus-zenfone-45' 
		else regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*specialoffer/',''),'\\.html.*',''),'-lite.*','') end as offer, 
	case
		when referrer_url like '%kaidee%' then 'kd'
		when referrer_url like '%facebook%' then 'fb' else 'oth' end as source,
	case 
		when action_id like '%online%' then 'online' 
		when action_id like '%callcenter%' then 'callcenter' 
		when action_id like '%line%' then 'line'  
		end as ful_channel, year, month, day  
from 
(
select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sight_date, b.value as tapad_id, a.action_id as action_id, case when lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform,  a.header.user_agent as user_agent, cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 + cast(split_part(a.header.ip_address,'.',2) as INT)*65536 + cast(split_part(a.header.ip_address,'.',3) as INT)*256+ cast(split_part(a.header.ip_address,'.',4) as INT) ip_number, a.header.referrer_url as referrer_url, a.year, a.month, a.day 

 from default.tracked_events a, a.header.incoming_ids b where a.property_id = '2868' and (a.action_id like '%submit%' or a.action_id like '%select%' or a.action_id ='undefined') and a.header.referrer_url like '%specialoffer%' and a.year=${var:year} and a.month=${var:month} and a.day =${var:day}) D ) E group by sight_date, tapad_id, carrier, offer,  source, ful_channel) F 
 on c.sight_date=f.sight_date and c.tapad_id=f.tapad_id and c.carrier=f.carrier and c.offer=f.offer and c.source = f.source  ;


 /*
 select offer, sum(imps), count(distinct tapad_id), sum(submits) from meas_ana.meas_table where sight_date < '2016-11-14' and sight_date >= '2016-11-01' and source ='kd' group by offer ;
 */
 
