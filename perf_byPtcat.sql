select pt_cat, sum(imp_flg) as imps, sum(click_flg) as clicks from 
(select 	case 
		when h.value like '%highlight1%' then 'cat1'
		when h.value like '%topbanner1%' then 'cat3'
		when h.value like '%topcomment%' then 'cat6'
	end as pt_cat, 
	case when action_id ='impression' then 1 else 0 end as imp_flg, 
	case when action_id ='click' then 1 else 0 end as click_flg 
from default.taps a, a.header.incoming_ids b, a.header.query_params q, a.header.http_headers h where q.key='ext_cat' and h.key = 'Referer' and a.campaign_id in (5413) and a.action_id IN ('impression','click') and a.year = 2017) C where pt_cat is not null group by pt_cat order by pt_cat asc ;

select pt_cat, sum(landing_flg) as landings, sum(select_flg) as selects, sum(submit_flg) as submits from 
(select 
	case 
		when a.header.referrer_url like '%pantipc1_display%' then 'cat1' 
		when a.header.referrer_url like '%pantipc3_display%' then 'cat3'
		when a.header.referrer_url like '%pantipc6_display%' then 'cat6' 
	end as pt_cat,
	case when action_id ='undefined' then 1 else 0 end as landing_flg, 
	case when action_id like '%select%' then 1 else 0 end as select_flg, 
    case when action_id like '%submit%' then 1 else 0 end as submit_flg

 from default.tracked_events a, a.header.incoming_ids b where a.property_id = '2868' and (a.action_id like '%submit%' or a.action_id like '%select%' or a.action_id ='undefined') and a.header.referrer_url like '%pantip%' and year=2017 ) C where pt_cat is not null group by pt_cat order by pt_cat asc ; 
