/*
####################################################################################
# Name: techname_bytpid_meas
# Description: Create impclick_daydif and impb4click by offer, tpid 
# Input: taps
# Version:
#   2016/12/01 RS: Initial version 
####################################################################################
*/

drop table if exists meas_ana.action_byoffer_tpid_2017;
use meas_ana;create table meas_ana.action_byoffer_tpid_2017 row format delimited fields terminated by '\t' as ( 

select case 
		when campaign_id = 5138 then 'kd'
		when campaign_id = 5413 then 'pt'
		end as source, 
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
	b.value as tapad_id, 
	cast(a.header.created_at/1000 as timestamp) as time, 
	a.action_id as action_id 
 from default.taps a, a.header.incoming_ids b, a.header.query_params q where q.key='ext_cat' and a.year=2017 and a.campaign_id in (5138, 5413) and a.action_id IN ('impression','click') order by source asc, offer asc, tapad_id asc, time asc);
 
 
drop table if exists meas_ana.impb4click_byoffer_tpid_2017;
use meas_ana;create table meas_ana.impb4click_byoffer_tpid_2017 row format delimited fields terminated by '\t' as ( 
 select a.source, a.offer, a.tapad_id, count(distinct time) as impb4click, datediff(b.click_moment, min(time)) as impclick_daydif 
from meas_ana.action_byoffer_tpid_2017 a
join (select source, 
		 offer, 
         tapad_id,
         min(time) as click_moment, 
		 regexp_replace(cast(min(time) as string),' .*','') as click_date 
        from meas_ana.action_byoffer_tpid_2017 where action_id = 'click'
        group by source, offer, tapad_id 
        ) b on a.source = b.source and a.tapad_id = b.tapad_id and a.offer=b.offer and a.time < b.click_moment where action_id='impression' 
		
		group by a.source, a.offer, a.tapad_id, b.click_moment order by source asc, offer asc, tapad_id asc);
		
