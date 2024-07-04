SELECT shipper_id 
       ,shipper_name 
       ,city_name 
       ,seniority
       ,coalesce(tier,'other') as tier  
       ,working_type 
       ,main_phone
       ,2022 - cast(year_birth as bigint) as age
       ,case when a14_del > 0 then 1 else 0 end as a14_ 
       ,case when a7_del > 0 then 1 else 0 end as a7_



FROM 
(SELECT sm.shipper_id
,sm.shipper_name
,sm.city_name
,date(from_unixtime(ps.create_time - 3600)) as onboard_date
,date_diff('day',date(from_unixtime(ps.create_time - 3600)),current_date) as seniority
,date(from_unixtime(sm.last_order_timestamp - 3600)) as last_active_date
--,coalesce(date_diff('day',date(from_unixtime(sm.last_order_timestamp - 3600)),current_date - interval '1' day),0)as non_active
,current.current_driver_tier as tier 
,ps.gender
,ct.main_phone
,substr(cast(ps.birth_date as varchar),1,4) as year_birth
,case when sm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as working_type
,sum(case when driver.report_date between current_date - interval '14' day and current_date - interval '1' day then total_order else null end) as a14_del
,sum(case when driver.report_date between current_date - interval '7' day and current_date - interval '1' day then total_order else null end) as a7_del


from 
shopeefood.foody_mart__profile_shipper_master sm 

---ado
left join 
(SELECT date(from_unixtime(dot.real_drop_time - 3600)) as report_date
,year(date(from_unixtime(dot.real_drop_time - 3600)))*100 + week(date(from_unixtime(dot.real_drop_time - 3600))) as create_year_week
,dot.uid
,dot.pick_city_id
, count(dot.ref_order_code) as total_order
FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
where dot.order_status = 400
--and dot.uid = 8644811
--and date(from_unixtime(dot.real_drop_time - 3600)) >= date((current_date) - interval '30' day)
--and date(from_unixtime(dot.real_drop_time - 3600)) < date(current_date)


group by 1,2,3,4)driver on driver.uid = sm.shipper_id

left join shopeefood.foody_internal_db__shipper_info_personal_tab__reg_continuous_s0_live ps on ps.uid = sm.shipper_id
left join shopeefood.foody_internal_db__shipper_info_contact_tab__reg_daily_s2_live ct on ct.uid = sm.shipper_id

--- tier 
LEFT JOIN
(SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
,bonus.uid as shipper_id
,case when hub.shipper_type_id = 12 then 'Hub'
when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
when bonus.tier in (3,8,13) then 'T3'
when bonus.tier in (4,9,14) then 'T4'
when bonus.tier in (5,10,15) then 'T5'
else 'other' end as current_driver_tier
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
)current on current.shipper_id = sm.shipper_id


where 1 = 1 
--and sm.shipper_id = 2996387
and sm.grass_date = 'current'
and sm.shipper_status_code = 1
group by 1,2,3,4,5,6,7,8,9,10,11)