SELECT shipper_id,shipper_name,city_name,order_status,working_status
-- ,main_phone
,last_active_date
,non_active
,case when non_active between 1 and 5 then '1. 1 - 5 days' 
      when non_active between 5 and 10 then '1. 5 - 10 days'
      when non_active between 10 and 14 then '2. 10 - 14 days'
      when non_active > 14 then '3. > 14 days'
      else '4. Normal' end as non_active_group
FROM 
(SELECT sm.shipper_id
,sm.shipper_name
,sm.city_name
,date(from_unixtime(ps.create_time - 3600)) as onboard_date
-- ,date(from_unixtime(sm.last_order_timestamp - 3600)) as last_active_date
,coalesce(date_diff('day',date(from_unixtime(sm.last_order_timestamp - 3600)),date(current_date)-interval '1' day),0)as non_active
,ct.main_phone
,ct.personal_email
,rt.identity_number
,ps.first_name
,ps.last_name
,ps.birth_date
,ps.gender
,ps.bike_number_plate
,case
WHEN st.shift_category = 1 and wt.start_time/3600 = 10 then '5 hour shift S'
WHEN st.shift_category = 1 then '5 hour shift'
WHEN st.shift_category = 2 then '8 hour shift'
WHEN st.shift_category = 3 then '10 hour shift'
ELSE 'Part-time' END AS hub_type
,wt.start_time/3600 as start_time
,wt.end_time/3600 as end_time
,hi.hub_name
,ct.current_address
,current.current_driver_tier
,case when ps.take_order_status = 1 then 'Normal'
when ps.take_order_status = 3 then 'Pending'
else 'N/A' end as order_status 
, case when working_status = 1 then 'Normal'
else 'Off' end as working_status
,case when ps.take_order_status = 3 then date(from_unixtime(pd.update_time - 3600)) 
else null end as pending_date
,coalesce(sum(driver.total_order),0) as total_order
,coalesce(count(distinct driver.report_date),0) as active_date
,coalesce(max(driver.report_date),null) as last_active_date

from 
shopeefood.foody_mart__profile_shipper_master sm 

---ado
left join 
(SELECT date(from_unixtime(dot.real_drop_time - 3600)) as report_date
,year(date(from_unixtime(dot.real_drop_time - 3600)))*100 + week(date(from_unixtime(dot.real_drop_time - 3600))) as create_year_week
,dot.uid
,dot.pick_city_id
, count(dot.ref_order_code) as total_order
FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot
where dot.order_status = 400 
--and dot.uid = 8644811
--and date(from_unixtime(dot.real_drop_time - 3600)) >= date((current_date) - interval '30' day)
--and date(from_unixtime(dot.real_drop_time - 3600)) < date(current_date)
-- and date(from_unixtime(dot.real_drop_time - 3600)) between date('2021-08-22') - interval '90' day and date('2021-08-22')

group by 1,2,3,4)driver on driver.uid = sm.shipper_id

left join shopeefood.foody_internal_db__shipper_info_contact_tab__reg_daily_s2_live ct on ct.uid = sm.shipper_id
left join shopeefood.foody_internal_db__shipper_info_personal_tab__reg_daily_s2_live ps on ps.uid = sm.shipper_id
--pending date 
left join shopeefood.foody_internal_db__shipper_log_pending_reason_tab__reg_daily_s0_live pd on pd.uid = sm.shipper_id
left join shopeefood.foody_internal_db__shipper_log_pending_reason_tab__reg_daily_s0_live filter on filter.uid = pd.uid and pd.update_time < filter.update_time

left join shopeefood.foody_internal_db__shipper_registration_tab__reg_daily_s0_live rt on sm.shipper_id = cast(json_extract(rt.extra_data,'$.shipper_uid') as bigint)
--- tier 
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
)current on current.shipper_id = sm.shipper_id
---SHIFT
LEFT JOIN ( shopeefood.foody_internal_db__shipper_info_work_tab__reg_daily_s0_live ) st on st.uid = sm.shipper_id
LEFT JOIN shopeefood.foody_internal_db__shipper_config_working_time_tab__vn_daily_s0_live wt on wt.id = st.working_time_id
---HUB
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_mapping_tab__reg_daily_s0_live hm on hm.uid = sm.shipper_id 
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live hi on hi.id = hm.hub_id 
where 1 = 1 
and filter.uid is null
and sm.city_id in (219)
and sm.grass_date = 'current'
-- and sm.shipper_type_id = 12
and sm.shipper_status_code = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)

   
