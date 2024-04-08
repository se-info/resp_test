SELECT 
date(from_unixtime(a.report_date - 3600)) as date_ 
,a.uid as shipper_id 
,sm.city_name
,case when sm.shipper_type_id = 12 then 'Hub'
when a.tier in (1,6,11) then 'T1' when a.tier in (2,7,12) then 'T2'
when a.tier in (3,8,13) then 'T3'
when a.tier in (4,9,14) then 'T4'
when a.tier in (5,10,15) then 'T5'
else null end as current_driver_tier
--,if(a.daily_point < c.daily_point and a.total_point = b.total_point,1,0) as flag 
,c.total_point as point_l30d
,a.total_point as total_point_current
,a.total_point - c.total_point as diff_w_30d
--,(a.total_point - b.total_point ) as diff_w_l1d

from shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live a 

---Last 1 Day 
left join shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live b 
on b.uid = a.uid  and date(from_unixtime(b.report_date - 3600)) = date(from_unixtime(a.report_date - 3600)) - interval '1'day

---Last 30 Day 
left join 
(SELECT 
uid,sum(total_earned_points) as total_point
FROM 
shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live 
where date(from_unixtime(report_date - 3600)) between current_date - interval '31' day and current_date - interval '2' day 
group by 1
)c 
on c.uid = a.uid 
---Driver Info 
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.uid and try_cast(grass_date as date) = date(from_unixtime(a.report_date - 3600))
where 1 = 1 
and shipper_status_code = 1 
and sm.shipper_type_id = 11
and date(from_unixtime(a.report_date - 3600)) = current_date - interval '1' day --and current_date - interval '1' day

ORDER BY shipper_id,date(from_unixtime(a.report_date - 3600)) asc 