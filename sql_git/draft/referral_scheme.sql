with hub AS
(SELECT shipper_id
,min(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) first_day_in_hub
,max(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) last_day_in_hub
from shopeefood.foody_mart__profile_shipper_master

where 1=1
and grass_region = 'VN'
and shipper_type_id = 12
group by 1
)
,driver as 
(
    
SELECT base.shipper_id
,current.shipper_name
,current.city_name
,base.first_day_in_hub
--,year(base.first_day_in_hub)*100 + week(base.first_day_in_hub) as week_in_hub
--,case when current.shipper_type_id = 11 then year(base.last_day_in_hub)*100 + week(base.last_day_in_hub) ELSE NULL end as week_out_hub
,case when fresh.id is not null then '1. Fresh' else '2. PT16' end as source 
--,case when current.shipper_type_id = 11 then base.last_day_in_hub ELSE NULL end as last_day_in_hub
--, case when current.shipper_type_id = 11 then 'pt-16'
--when current.shipper_type_id = 12 then 'Hub'
--else NULL end as working_group
,blt.current_driver_tier as first_tier_in_hub
,case when typ.shift_hour is not null then typ.shift_hour 
      else sst.shift_hour end as shift_hour 
      


FROM (SELECT * FROM hub) base

LEFT JOIN
(SELECT shipper_id
,shipper_shift_id
,shipper_name
,city_name
,case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end as report_date
,shipper_type_id
,shipper_status_code
from shopeefood.foody_mart__profile_shipper_master

where 1=1
and grass_region = 'VN'
and grass_date = 'current'
group by 1,2,3,4,5,6,7
)current on base.shipper_id = current.shipper_id 

---check type

LEFT JOIN
(SELECT 
uid,(end_time - start_time)/3600 as shift_hour ,date_ts, row_number() over(partition by uid order by date_ts asc) as rank 

from shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live

group by 1,2,3
)typ  on base.shipper_id = typ.uid  

----type add on 
LEFT JOIN 
(SELECT shipper_id
,shipper_shift_id
,row_number() over(partition by shipper_id order by report_date  asc) as rank

from (SELECT *, case when grass_date = 'current' then date(current_date)
                else cast(grass_date as date) end as report_date
      from    shopeefood.foody_mart__profile_shipper_master
      where grass_region = 'VN')

where 1=1
and shipper_type_id = 12
 )add on add.shipper_id = base.shipper_id 

--- Hub Source
LEFT JOIN vnfdbi_opsndrivers.foody_hub_fresh_driver_data fresh on cast(fresh.id as bigint) = base.shipper_id 

---Shift Info
LEFT JOIN ( 
SELECT
id,date_format(from_unixtime(start_time - 25200),'%H') as start_time
,date_diff('hour',date_trunc('hour',from_unixtime(start_time - 3600)),date_trunc('hour',from_unixtime(end_time-3600))) as shift_hour
FROM shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live
) sst on sst.id = add.shipper_shift_id

-----bonus points

LEFT JOIN (
SELECT
    blt.uid
    , CAST("from_unixtime"((blt.report_date - 3600)) AS date) report_date
    , blt.total_point
    , CASE WHEN blt.tier IN (1, 6, 11) THEN 'T1'
        WHEN blt.tier IN (2, 7, 12) THEN 'T2' 
        WHEN blt.tier IN (3, 8, 13) THEN 'T3' 
        WHEN blt.tier IN (4, 9, 14) THEN 'T4' 
        WHEN blt.tier IN (5, 10, 15) THEN 'T5' 
        ELSE null END current_driver_tier
    FROM
    shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live blt
) blt ON blt.uid = base.shipper_id AND blt.report_date = base.first_day_in_hub - interval '1' day


where 1 = 1 
and current.shipper_status_code = 1 
and typ.rank = 1
and add.rank = 1

GROUP BY 1,2,3,4,5,6,7
)

,ado as 
(SELECT base.report_date
,base.city_group
,base.uid
,base.shipper_name
,count(case when base.is_inshift = 1 then base.ref_order_id else null end ) as total_order
FROM     
(SELECT dot.uid
,psm.shipper_name
,case
WHEN dot.pick_city_id = 217 then 'HCM'
WHEN dot.pick_city_id = 218 then 'HN'
ELSE NULL end as city_group
,psm.city_name
,date(from_unixtime(dot.real_drop_time -3600)) as report_date
,YEAR(date(from_unixtime(dot.real_drop_time -3600)))*100 + WEEK(date(from_unixtime(dot.real_drop_time -3600))) as created_year_week
,case
WHEN st.shift_hour = 5 then '5 hour shift'
WHEN st.shift_hour = 8 then '8 hour shift'
WHEN st.shift_hour = 10 then '10 hour shift'
ELSE add.hub_type END AS hub_type
,case when date(from_unixtime(dot.real_drop_time -3600)) between date('2021-07-09') and date('2021-10-05') and psm.shipper_type_id = 12 and dot.pick_city_id = 217 then 1
when date(from_unixtime(dot.real_drop_time -3600)) between date('2021-07-24') and date('2021-10-04') and psm.shipper_type_id = 12 and dot.pick_city_id = 218 then 1
when cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_inshift
,dot.delivery_cost*1.00/100 as delivery_cost
,dot.ref_order_id
,case when date(from_unixtime(dot.real_drop_time -3600)) between date('2021-07-09') and date('2021-10-05') and dot.pick_city_id = 217 then 10000
when date(from_unixtime(dot.real_drop_time -3600)) between date('2021-09-18') and date('2021-10-04') and dot.pick_city_id = 218 then 10000
else 0 end as bonus
,coalesce(cast(json_extract(order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as bigint ),0) as bwf
,case when real_drop_time > estimated_drop_time then 1
else 0 end as is_late_eta

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN
(
select *
,Case
when grass_date = 'current' then date(current_date)
else cast(grass_date AS DATE ) END as report_date
from shopeefood.foody_mart__profile_shipper_master
WHERE grass_region = 'VN'
) psm on psm.shipper_id = dot.uid AND psm.report_date = date(from_unixtime(dot.real_drop_time -3600))
left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on oct.id = dot.ref_order_id
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet on dot.id = doet.order_id

LEFT JOIN (SELECT *,date(from_unixtime(date_ts - 3600)) as report_date
,(end_time - start_time)/3600 as shift_hour
from
shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live)st on st.uid = dot.uid and st.report_date = date(from_unixtime(dot.real_drop_time -3600))

---shift add on
LEFT JOIN ( SELECT *,case
WHEN shift_hour = 5 then '5 hour shift'
WHEN shift_hour = 8 then '8 hour shift'
WHEN shift_hour = 10 then '10 hour shift'
ELSE null END AS hub_type
FROM
(select
id,date_format(from_unixtime(start_time - 25200),'%H') as start_time
,date_diff('hour',date_trunc('hour',from_unixtime(start_time - 3600)),date_trunc('hour',from_unixtime(end_time-3600))) as shift_hour
FROM shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live)
) add on add.id = psm.shipper_shift_id


WHERE 1=1
AND dot.pick_city_id in (217,218)
AND psm.shipper_type_id in (12)
and dot.ref_order_category = 0
and psm.city_id in (217,218)
AND dot.order_status = 400
AND date(from_unixtime(dot.real_drop_time -3600)) >= date((current_date) - interval '30' day)
and date(from_unixtime(dot.real_drop_time -3600)) < date(current_date)
GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13)base
where base.is_inshift = 1

GROUP BY 1,2,3,4
)


select   d.shipper_id 
        ,d.shipper_name
        ,d.first_day_in_hub
        ,d.first_tier_in_hub
        ,d.city_name  
        ,count(distinct case when e.report_date between d.first_day_in_hub and d.first_day_in_hub + interval '6' day then e.report_date else null end) as working_day
        ,sum(case when e.report_date between d.first_day_in_hub and d.first_day_in_hub + interval '6' day then e.total_order else null end) as total_order  



from driver d 

left join ado e on e.uid = d.shipper_id 
---and e.report_date between d.first_day_in_hub - interval '6' day and d.first_day_in_hub 


where d.first_day_in_hub between date'2022-04-01' and date'2022-04-20'
--and d.shipper_id = 21922727
group by 1,2,3,4,5






