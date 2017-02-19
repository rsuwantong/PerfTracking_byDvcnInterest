select * from sgmt_rules.feedback_tbl limit 5;

select * from meas_ana.meas_table_2017 limit 5;


select * from apollo.dtac_vertical_dataset where event_source ='Tap' and campaign_id in (5413) and year = 2017 and platform ='IPHONE' limit 20;

drop table if exists sgmt_rules.feedback_temp;
create table sgmt_rules.feedback_temp row format delimited fields terminated by '\t' as (
select offer, source, platform, marketing_name, vendor, lcase(model) as model, camera_pixels, year_released, diagonal_screen_size, sum(imp_flg) as imps, sum(click_flg) as clicks, sum(landing_flg) as landings, sum(submit_flg) as submits, count(distinct tapad_id) as reach from
	(select 	regexp_replace(cast(cast(created_at/1000 as timestamp) as string),' .*','') as sighted_date, 
			tapad_id,
case	
				when tactic_id = 186858 then 'mnp-device-discount-samsung'
				when tactic_id = 191242 then 'mnp-device-discount-samsung'
				when tactic_id = 191243 then 'mnp-free-device'
				when tactic_id = 191244  then 'tariff'
				when tactic_id = 199183 then 'booster'
				when tactic_id in (197236, 213768, 241984) then 'mnp-samsung-galaxy-j2'
				when tactic_id = 200320 then 'mnp-samsung-galaxy-j5'
				when tactic_id = 201014 then 'mnp-samsung-galaxy-a5'
				when tactic_id in (203164,217118) then 'mnp-asus-zenfone-45' 
				when tactic_id = 214301 then 'mnp-oppo-mirror5' 
				when tactic_id = 217958 then 'mnp-free-dtac-pocket-wifi' 
				when tactic_id = 223067 then 'mnp-vivo-v5' 
				when tactic_id = 221701 then 'mnp-free-dtac-phone-s2'	
				when tactic_id in (222299, 231134, 231808, 231809, 238494, 238495, 238496) then 'mnp-samsung-galaxy-j5-prime'
				when tactic_id in (224006, 231132, 231810, 231811) then 'mnp-asus-zenfone-55'
				when tactic_id in (226384, 232254, 232255, 232257, 238491, 238492, 238493, 238924, 241140,241141) then 'mnp-samsung-galaxy-j2-prime'
				when tactic_id in (232303, 232259, 232260, 232261, 238498, 238501, 238502) then 'mnp-samsung-galaxy-j7-prime'
				when tactic_id in (231135, 231805, 231806) then 'mnp-huawei-mate9'
				when tactic_id in (231460, 231812, 231813) then 'sim-platinum-number'
				when tactic_id in (231461, 231814, 231815, 241982) then 'sim-nice-number'
				when tactic_id in (231462, 231816, 231817) then 'lucky-number'
				when tactic_id in (232262, 232263, 232264, 241136, 241137, 241138, 241983) then 'mnp-huawei-p9'
				when tactic_id in (234395, 234396, 234397) then 'mnp-samsung-galaxy-note5'
				when tactic_id in (238642, 238644, 238645, 241985) then 'mgm'
				when referrer_url like '%special-package%' then 'tariff' 
				when referrer_url like '%asus-zenfone-45%' then 'mnp-asus-zenfone-45' 
				when referrer_url like '%mnp-huawei-p9%' then 'mnp-huawei-p9' 
				else lcase(regexp_replace(regexp_replace(regexp_replace(regexp_replace(referrer_url,'.*specialoffer/',''),'\\.html.*',''),'-lite.*',''),'-v[0-9].*',''))
				end as offer,
			tactic_id, referrer_url, 
			case 
				when campaign_id = 5138 or referrer_url like '%kaidee%' then 'kd' 
				when campaign_id = 5413 or referrer_url like '%pantip%' then 'pt' 
				when referrer_url like '%facebook%' then 'fb' else 'other'
				end as source,
			case 
				when platform='IPHONE' and (lower(user_agent) like ('%windows phone%') or lower(user_agent) like ('%lumia%')) then 'WINDOWS_PHONE' 
				else platform end as platform,
			carrier, 
			case 
				when platform = 'IPHONE' and lcase(user_agent) like '%cpu iphone os%' or lcase(user_agent) like '%iphone; u; cpu iphone%' or lcase(user_agent) like '%iphone; cpu os%' and lcase(platform)='iphone' then regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*iphone;( u;)? cpu ',''),'like mac os.*',''),'_.*','') 
				when lcase(user_agent) like '%(null) [fban%' and lcase(user_agent) like '%fbdv/iphone%' and lcase(platform)='iphone' then regexp_extract(regexp_replace(lcase(user_agent),'.*fbdv/',''),'iphone[0-9]',0) 
				else marketing_name 
				end as marketing_name,
			vendor,
			case 
				when platform = 'IPHONE' and lcase(user_agent) like '%cpu iphone os%' or lcase(user_agent) like '%iphone; u; cpu iphone%' or lcase(user_agent) like '%iphone; cpu os%' and lcase(platform)='iphone' then regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*iphone;( u;)? cpu ',''),'like mac os.*',''),'_.*','') 
				when lcase(user_agent) like '%(null) [fban%' and lcase(user_agent) like '%fbdv/iphone%' and lcase(platform)='iphone' then regexp_extract(regexp_replace(lcase(user_agent),'.*fbdv/',''),'iphone[0-9]',0) 
				when marketing_name ="" and vendor ="" and model ="" then trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lcase(user_agent),'.*android [0-9](.[0-9](.[0-9])?)?; ',''),' build.*|; android/.*|\\) 
				applewebkit.*|/v[0-9] linux.*|v_td.*|_td/v[0-9].*|i_style.*',''),'.*(th|en|zh|zz)(-|_)(gb|au|ph|th|us|cn|nz|gb|tw|fi|jp|za|sg|ie|zz);? |.*nokia; ',''),'/.*|linux.*','')),'[^0-9a-z\- \.]',''),'.*samsung(-| )|.*lenovo |.*microsoft |.*th- ',''),'like.*|lollipop.*',''),' applewebkit.*',''),' dual sim.*',''))
				else model 
			end as model,
			camera_pixels,
			year_released,
			diagonal_screen_size,
			case when action_id ='impression' then 1 else 0 end as imp_flg, 
			case when action_id ='click' then 1 else 0 end as click_flg,
		    case when action_id ='undefined' and event_source = 'TrackedEvent' then 1 else 0 end as landing_flg,  
		    case when action_id like '%submit%' and event_source = 'TrackedEvent' then 1 else 0 end as submit_flg, 
			year, month, day 
			from apollo.dtac_vertical_dataset where event_source IN ('Tap', 'TrackedEvent' ) and (campaign_id in (5138, 5413) or url REGEXP 'ta_property_id=2868') and year = 2017 and (action_id IN ('impression','click','undefined') or action_id like '%submit%')  ) A where offer !="" and model !="" group by offer, source, platform, marketing_name, vendor, model, camera_pixels, year_released, diagonal_screen_size );
			
/*iphone7 lovers*/	
select 'iphone7' as offer, 1 as offer_priority, model from sgmt_rules.feedback_temp where offer = "mnp-iphone7" and source in ('fb') and submits>=1 and char_length(model)>2 and model not like '%iphone os%' and model not like '%firefox%' and model not like '%generic android mobile%';

/*p9 lovers*/
select 'p9' as offer, 2 as offer_priority, model from sgmt_rules.feedback_temp where offer = "mnp-huawei-p9" and source in ('fb') and submits>=10 and char_length(model)>2 and model not like '%iphone os%' and model not like '%firefox%' and model not like '%generic android mobile%';

/*j7-prime*/
select 'j7-prime' as offer, 3 as offer_priority, model from 
(select distinct dvc_techname as model from apollo_util.techname_prop_map where cast(release_price as double)*40 >= 20000 and char_length(dvc_techname)>2 and dvc_techname not like '%iphone%') A;

drop table if exists sgmt_rules.offer_170207_fullist;
create table sgmt_rules.offer_170207_fullist row format delimited fields terminated by '\t' as (
select * from  
(select 'iphone7' as offer, 1 as offer_priority, model from sgmt_rules.feedback_temp where offer = "mnp-iphone7" and source in ('fb') and submits>=1 and char_length(model)>2 and model not like '%iphone os%' and model not like '%firefox%' and model not like '%generic android mobile%' and model not like '%windows%' union all
select 'p9' as offer, 2 as offer_priority, model from sgmt_rules.feedback_temp where offer = "mnp-huawei-p9" and source in ('fb') and submits>=10 and char_length(model)>2 and model not like '%iphone os%' and model not like '%firefox%' and model not like '%generic android mobile%' and model not like '%windows%' union all 
select 'j7-prime' as offer, 3 as offer_priority, model from 
(select distinct dvc_techname as model from apollo_util.techname_prop_map where cast(release_price as double)*40 >= 20000 and char_length(dvc_techname)>2 and dvc_techname not like '%iphone%') A) B order by offer_priority asc);

select offer_priority, concat('userAgent(".*(?i)(',group_concat(trim(model),'|'),').*")') as rule from 
(select min(offer_priority) as offer_priority, model from sgmt_rules.offer_170207_fullist group by model) A group by offer_priority order by offer_priority asc ;



/*j7-prime*/
select concat('userAgent(".*(?i)(',group_concat(trim(model),'|'),').*")') as rule from 
(select distinct dvc_techname as model from apollo_util.techname_prop_map where cast(release_price as double)*40 >= 20000 and char_length(dvc_techname)>2 and dvc_techname not like '%iphone%') A;




apollo_util.techname_prop_map b on a.target_rlp_min <=cast(b.release_price as double)*40 and cast(b.release_price as double)*40<=a.target_rlp_max group by offer, uc1_priority, active_flg_kd, active_flg_fb, active_flg_pt, dvc_techname, min_CTR, min_landings, min_submits, min_S2L, max_CPS);



select concat('userAgent(".*(?i)(',group_concat(trim(model),'|'),').*")') from (select distinct model from sgmt_rules.feedback_temp where offer like '%huawei%' and landings>=1 and char_length(model)>2 and model not like '%chrome%' and model not like '%windows%' and model not like '%firefox%' and model not like '%safari%') A;

select model from sgmt_rules.feedback_temp where offer = "mnp-samsung-galaxy-j2-prime" and source in ('kd') and char_length(model)>2 ;

select distinct action_id from apollo.dtac_vertical_dataset where event_source in ('TrackedEvent' ) and (url REGEXP 'ta_property_id=2868') and year = 2017  and action_id !='undefined' and action_id not like '%select%' order by action_id asc;
		
		select count(distinct tapad_id) from apollo.dtac_vertical_dataset where event_source in ('Tap', 'TrackedEvent' ) and (campaign_id in (5138, 5413) or url REGEXP 'ta_property_id=2868') and year = 2017 ;