with raw as 
(select  
       sm.shipper_id
      ,sp.shopee_uid 
      ,sp.gender
      ,sm.shipper_name 
      ,sm.city_name
    --   ,coalesce(current.current_driver_tier,'Others') as tier_type
      ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as working_group
      ,date(from_unixtime(sp.create_time - 3600)) as onboard_date
      ,date_diff('day',date(from_unixtime(sp.create_time - 3600)),current_date - interval '1' day) as seniority
      --,min(case when ado.report_date between current_date - interval '29' day and current_date - interval '1' day then ado.report_date else null end) as min_date
      ,coalesce(sum(case when ado.report_date between current_date - interval '1' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l1d
      ,coalesce(sum(case when ado.report_date between current_date - interval '1' day and current_date - interval '1' day then ado.total_distance else null end),0) as total_distance_l1d
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '7' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l7d
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '15' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l15d
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '30' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l30d
    --   ,coalesce(sum(case when ado.report_date between current_date - interval '60' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l60d
      ,coalesce(sum(case when ado.report_date between current_date - interval '90' day and current_date - interval '1' day then ado.total_distance else null end),0) as total_distance_l90d
      ,coalesce(sum(case when ado.report_date between current_date - interval '90' day and current_date - interval '1' day then ado.total_order else null end),0) as total_order_l90d



from
shopeefood.foody_mart__profile_shipper_master sm

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live sp on sp.uid = sm.shipper_id 

---ado
left join
(SELECT 
date(from_unixtime(dot.real_drop_time - 3600)) as report_date
,year(date(from_unixtime(dot.real_drop_time - 3600)))*100 + week(date(from_unixtime(dot.real_drop_time - 3600))) as create_year_week
,dot.uid
,sum(dot.delivery_distance/cast(1000 as double)) as total_distance
, count(dot.ref_order_code) as total_order
FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot
where dot.order_status = 400

--and dot.uid = 8644811
and date(from_unixtime(dot.real_drop_time - 3600)) >= date((current_date) - interval '180' day)
--and date(from_unixtime(dot.real_drop_time - 3600)) < date(current_date)
--and date(from_unixtime(dot.real_drop_time - 3600)) between date('2021-10-27') and date('2021-11-10')

group by 1,2,3)ado on ado.uid = sm.shipper_id

---tier 
LEFT JOIN
(SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
,bonus.uid as shipper_id
,case when hub.shipper_type_id = 12 then 'Hub'
when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
when bonus.tier in (3,8,13) then 'T3'
when bonus.tier in (4,9,14) then 'T4'
when bonus.tier in (5,10,15) then 'T5'
else null end as current_driver_tier
,bonus.total_point
,bonus.daily_point

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

LEFT JOIN
(SELECT shipper_id
,shipper_type_id
,case when grass_date = 'current' then date(current_date)
else cast(grass_date as date) end as report_date

from shopeefood.foody_mart__profile_shipper_master

where 1=1
and (grass_date = 'current' OR cast(grass_date as date) >= date('2019-01-01'))
GROUP BY 1,2,3
)hub on hub.shipper_id = bonus.uid and hub.report_date = cast(from_unixtime(bonus.report_date - 60*60) as date)

where cast(from_unixtime(bonus.report_date - 60*60) as date) = date(current_date) - interval '1' day
)current on sm.shipper_id = current.shipper_id


where sm.grass_date = 'current'
and sm.shipper_status_code = 1 
--and sm.shipper_id = 2996387
and sm.city_name not like '%Test%'


group by 1,2,3,4,5,6,7,8
)

select  * 

from raw 

where gender = 'F'
