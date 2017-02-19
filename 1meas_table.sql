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


drop table if exists meas_ana.meas_table_2017;

create table meas_ana.meas_table_2017 
(sighted_date STRING, tapad_id STRING, carrier STRING, offer STRING, source STRING, room_id INT, room_name STRING, pt_cat STRING, ful_channel STRING, 
imps BIGINT, clicks BIGINT, landings BIGINT, selects BIGINT, submits BIGINT );

insert into meas_ana.meas_table_2017 

select case when c.sighted_date is not null then c.sighted_date else f.sighted_date end as sighted_date, 
		case when c.tapad_id is not null then c.tapad_id else f.tapad_id end as tapad_id, 
		case when c.carrier is not null then c.carrier else f.carrier end as carrier, 
		case when c.offer is not null then c.offer else f.offer end as offer, 
		case when c.source is not null then c.source else f.source end as source, 
		c.room_id, c.room_name, c.pt_cat, f.ful_channel, c.imps, c.clicks, f.landings, f.selects, f.submits from 
 (select sighted_date, tapad_id, carrier, offer, source, room_id, room_name, pt_cat,   
sum(imp_flg) as imps, sum(click_flg) as clicks  from 
(select sighted_date, tapad_id, 
	case when action_id ='impression' then 1 else 0 end as imp_flg, 
    case when action_id ='click' then 1 else 0 end as click_flg, 		
	case when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier, 
	offer, source, room_id,  
	case 
		when source ='kd' and regexp_replace(regexp_replace(regexp_replace(header_url,'.*kaidee\\.com/',''),'/.*',''),'\\?.*','') like 'c%' and header_url not like 'categories' then regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(header_url,'.*kaidee\\.com/',''),'/.*',''),'\\?.*',''),'.*[0-9]-',''),'-.*','') 
		when source ='pt' and header_url like '%highlight1%' then 'mainpage'
		when source ='pt' and header_url like '%topbanner1%' then regexp_replace(header_url, '.*topbanner1:', '')
		when source ='pt' and header_url like '%topcomment%' then regexp_replace(header_url, '.*topcomment:', '')
		end as room_name,  
	case 
		when source ='pt' and header_url like '%highlight1%' then 'cat1'
		when source ='pt' and header_url like '%topbanner1%' then 'cat3'
		when source ='pt' and header_url like '%topcomment:%' then 'cat6'
		when source ='pt' and header_url like '%topcomment2:%' then 'cat7'
	end as pt_cat 
	from 
(select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sighted_date, b.value as tapad_id, a.action_id as action_id, cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 + cast(split_part(a.header.ip_address,'.',2) as INT)*65536 + cast(split_part(a.header.ip_address,'.',3) as INT)*256+ cast(split_part(a.header.ip_address,'.',4) as INT) ip_number, 
	case	
		when a.tactic_id = 186858 then 'mnp-device-discount-samsung'
		when a.tactic_id = 191242 then 'mnp-device-discount-samsung'
		when a.tactic_id = 191243 then 'mnp-free-device'
		when a.tactic_id = 191244  then 'tariff'
		when a.tactic_id = 199183 then 'booster'
		when a.tactic_id in (197236, 213768, 241984) then 'mnp-samsung-galaxy-j2'
		when a.tactic_id = 200320 then 'mnp-samsung-galaxy-j5'
		when a.tactic_id = 201014 then 'mnp-samsung-galaxy-a5'
		when a.tactic_id in (203164,217118) then 'mnp-asus-zenfone-45' 
		when a.tactic_id = 214301 then 'mnp-oppo-mirror5' 
		when a.tactic_id = 217958 then 'mnp-free-dtac-pocket-wifi' 
		when a.tactic_id = 223067 then 'mnp-vivo-v5' 
		when a.tactic_id = 221701 then 'mnp-free-dtac-phone-s2'	
		when a.tactic_id in (222299, 231134, 231808, 231809, 238494, 238495, 238496) then 'mnp-samsung-galaxy-j5-prime'
		when a.tactic_id in (224006, 231132, 231810, 231811) then 'mnp-asus-zenfone-55'
		when a.tactic_id in (226384, 232254, 232255, 232257, 238491, 238492, 238493, 238924, 241140,241141) then 'mnp-samsung-galaxy-j2-prime'
		when a.tactic_id in (232303, 232259, 232260, 232261, 238498, 238501, 238502) then 'mnp-samsung-galaxy-j7-prime'
		when a.tactic_id in (231135, 231805, 231806) then 'mnp-huawei-mate9'
		when a.tactic_id in (231460, 231812, 231813) then 'sim-platinum-number'
		when a.tactic_id in (231461, 231814, 231815, 241982) then 'sim-nice-number'
		when a.tactic_id in (231462, 231816, 231817) then 'lucky-number'
		when a.tactic_id in (232262, 232263, 232264, 241136, 241137, 241138, 241983) then 'mnp-huawei-p9'
		when a.tactic_id in (234395, 234396, 234397) then 'mnp-samsung-galaxy-note5'
		when a.tactic_id in (238642, 238644, 238645, 241985) then 'mgm'
		end as offer, 
	cast(q.value as int) as room_id, 
	h.value as header_url, 
	case 
		when campaign_id = 5138 then 'kd' 
		when campaign_id = 5413 then 'pt' 
		end as source 
 from default.taps a, a.header.incoming_ids b, a.header.query_params q, a.header.http_headers h where q.key='ext_cat' and h.key = 'Referer' and a.campaign_id in (5138,5413) and a.action_id IN ('impression','click') and a.year = 2017  ) A) B group by sighted_date, tapad_id, carrier, offer, source, room_id, room_name, pt_cat) C 
 
 full outer join 
 
 (select  sighted_date, tapad_id, carrier, offer,  source, pt_cat, ful_channel,  
		sum(select_flg) as selects, sum(landing_flg) as landings, sum(submit_flg) as submits  from 
 
(select sighted_date, 
	   tapad_id, 
	   case when action_id ='undefined' then 1 else 0 end as landing_flg, 
	   case when action_id like '%select%' then 1 else 0 end as select_flg, 
       case when action_id like '%submit%' then 1 else 0 end as submit_flg, 		
	case 
		when ip_number between 18087936 and 18153471 then 'TOT' when ip_number between 19791872 and 19922943 then 'DTAC' when ip_number between 456589312 and 456654847 then  'TMH' when ip_number between 837156864 and 837222399 then  'AIS'when ip_number between 837615616 and 837681151 then  'TMH' when ip_number between 1848705024 and 1848770559 then  'AIS' when ip_number between 1867776000 and 1867825151 then  'DTAC' when ip_number between 1867826176 and 1867841535 then  'DTAC' when ip_number between 1933770752 and 1933836287 then  'DTAC' when ip_number between 1998520320 and 1998553087 then  'AIS' when ip_number between 2523597824 and 2523598847 then  'OTH' when ip_number between 3033972736 and 3033980927 then  'TMH' when ip_number between 3068657664 and 3068723199 then  'AIS' when ip_number between 3398768640 and 3398769663 then  'AIS' when ip_number between 3415276128 and 3415276159 then  'TMH' when ip_number between 3742892032 and 3742957567 then  'TMH' else 'Wi-Fi' end as carrier,
	case 
		when referrer_url like '%special-package%' then 'tariff' 
		when referrer_url like '%asus-zenfone-45%' then 'mnp-asus-zenfone-45' 
		when referrer_url like '%mnp-huawei-p9%' then 'mnp-huawei-p9' 
		else lcase(regexp_replace(regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*specialoffer/',''),'\\.html.*',''),'-lite.*',''),'-v[0-9].*','')) end as offer, 
	case
		when referrer_url like '%kaidee%' then 'kd'
		when referrer_url like '%pantip%' then 'pt'
		when referrer_url like '%facebook%' then 'fb' else 'oth' end as source,
	case 
		when referrer_url like '%pantip%' and referrer_url like '%pantipc1_display%' then 'cat1' 
		when referrer_url like '%pantip%' and referrer_url like '%pantipc3_display%' then 'cat3'
		when referrer_url like '%pantip%' and referrer_url like '%pantipc6_display%' then 'cat6' 
		when referrer_url like '%pantip%' and referrer_url like '%pantipc7_display%' then 'cat7' 
	end as pt_cat, 
	case 
		when action_id like '%online%' then 'online' 
		when action_id like '%callcenter%' then 'callcenter' 
		when action_id like '%line%' then 'line'  
		end as ful_channel 
from 
(
select regexp_replace(cast(cast(a.header.created_at/1000 as timestamp) as string),' .*','') as sighted_date, b.value as tapad_id, a.action_id as action_id, case when lower(a.header.platform)='iphone' and (lower(a.header.user_agent) like ('%windows phone%') or lower(a.header.user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' else a.header.platform end as platform,  a.header.user_agent as user_agent, cast(split_part(a.header.ip_address,'.',1) as INT)*16777216 + cast(split_part(a.header.ip_address,'.',2) as INT)*65536 + cast(split_part(a.header.ip_address,'.',3) as INT)*256+ cast(split_part(a.header.ip_address,'.',4) as INT) ip_number, a.header.referrer_url as referrer_url 

 from default.tracked_events a, a.header.incoming_ids b where a.property_id = '2868' and (a.action_id like '%submit%' or a.action_id like '%select%' or a.action_id ='undefined') and year=2017 ) D ) E group by sighted_date, tapad_id, carrier, offer,  source, pt_cat, ful_channel) F 
 on c.sighted_date=f.sighted_date and c.tapad_id=f.tapad_id and c.carrier=f.carrier and c.offer=f.offer and c.source = f.source ;


 /*
 select source, offer, sum(imps), count(distinct tapad_id), sum(submits) from meas_ana.meas_table_2017 group by source, offer order by source asc, offer asc;
 */
 
