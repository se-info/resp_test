with ado as
(
SELECT *

FROM
(SELECT
base2.report_date
,case when base2.report_date between date('2021-11-10') and date('2021-11-16') then 202101
              when base2.report_date between date('2021-11-03') and date('2021-11-09') then 202102
              else base2.created_year_week end as created_year_week
,base2.created_year_week as week_
,base2.uid
,base2.first_day_in_hub
,base2.city_group
,base2.hub_type
,base2.shipper_name
,base2.total_order
,base2.total_late
,base2.total_rating
,base2.total_order_rating
,base2.is_qualified_kpi
,cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT) as online_
,case when base2.report_date between date('2021-07-09') and date('2021-10-05') and base2.city_group = 'HCM' then base2.delivery_cost
when base2.report_date between date('2021-07-24') and date('2021-10-04') and base2.city_group = 'HN' then base2.delivery_cost
else coalesce(cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint),base2.total_order*13500) end as ship_shared

,case when base2.report_date between date('2021-07-09') and date('2021-10-05') and base2.city_group = 'HCM' then base2.bonus
when base2.report_date between date('2021-07-24') and date('2021-10-04') and base2.city_group = 'HN' then base2.bonus
else coalesce(cast(json_extract(hub.extra_data,'$.total_bonus') as bigint),0) end as daily_bonus


,case when cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then (cast(json_extract(hub.extra_data,'$.total_income') as bigint) - cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint))
else 0 end as extra_ship

,base2.bwf
,0 as surge

,case when base2.report_date between date('2021-07-09') and date('2021-10-05') and base2.city_group = 'HCM' then base2.delivery_cost + base2.bonus
when base2.report_date between date('2021-07-24') and date('2021-10-04') and base2.city_group = 'HN' then base2.delivery_cost + base2.bonus
else if(cast(json_extract(hub.extra_data,'$.total_order') as bigint) = base2.total_order,cast(json_extract(hub.extra_data,'$.total_income') as bigint),13500*base2.total_order) end as total_income
,case when base2.report_date >= date('2021-10-04') and base2.city_group = 'HN' then 1
      when base2.report_date >= date('2021-10-06') and base2.city_group = 'HCM' then 1
      else 0 end as is_resume
FROM
(
with hub AS
(SELECT shipper_id
,min(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) first_day_in_hub
,max(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) last_day_in_hub
from shopeefood.foody_mart__profile_shipper_master

where 1=1
and shipper_type_id = 12 and grass_region = 'VN'
group by 1)
SELECT base1.report_date
,base1.created_year_week
,base1.uid
,base1.shipper_name
,hub.first_day_in_hub
,case when hub.week_join_hub < (base1.created_year_week - 1) then 'Current Driver'
when hub.week_join_hub >= (base1.created_year_week - 1) then 'New Driver'
else null end as hub_seniority
,base1.city_group
,base1.hub_type
,base1.total_order
,base1.total_late
,base1.total_order_rating
,case WHEN base1.hub_type = '5 hour shift' AND base1.total_order < 16 and base1.city_group in ('HCM','HN') THEN 1
WHEN base1.hub_type = '5 hour shift' AND base1.total_order < 10 and base1.city_group = 'HP' THEN 1    
WHEN base1.hub_type = '8 hour shift' AND base1.total_order < 25 THEN 1
WHEN base1.hub_type = '10 hour shift' AND base1.total_order < 30 THEN 1
WHEN base1.hub_type = '3 hour shift' AND base1.total_order < 8 THEN 1 
ELSE 0 END as less_than_min
,case WHEN base1.hub_type = '5 hour shift' AND base1.total_order > 16 and base1.city_group in ('HCM','HN') THEN 1
WHEN base1.hub_type = '5 hour shift' AND base1.total_order > 10 and base1.city_group = 'HP' THEN 1
WHEN base1.hub_type = '8 hour shift' AND base1.total_order > 25  THEN 1    
WHEN base1.hub_type = '10 hour shift' AND base1.total_order >30 THEN 1
WHEN base1.hub_type = '3 hour shift' AND base1.total_order > 8 THEN 1 
ELSE 0 END AS over_min
,case WHEN base1.hub_type = '5 hour shift' AND base1.total_order = 16 and base1.city_group in ('HCM','HN') THEN 1
WHEN base1.hub_type = '5 hour shift' AND base1.total_order = 10 and base1.city_group = 'HP' THEN 1    
WHEN base1.hub_type = '8 hour shift' AND base1.total_order = 25 THEN 1
WHEN base1.hub_type = '10 hour shift' AND base1.total_order = 30 THEN 1
WHEN base1.hub_type = '3 hour shift' AND base1.total_order = 8 THEN 1 
ELSE 0 END AS pass_min

,case when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '10 hour shift'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/3600)/10 >= 0.9
and array_join(cast(json_extract(extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.00000000/3600 >= 2 then 1
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '8 hour shift'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/3600)/8 >= 0.9
and array_join(cast(json_extract(extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.000/3600 >= 2 then 1
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '5 hour shift'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.000000000/3600)/5 >= 0.9
and array_join(cast(json_extract(extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.00000000/3600 >= 1 then 1
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HCM'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.000000000/3600)/3 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' then 1 
--and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.000/3600 >= 1 then 1 
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HN'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000000/3600)/3 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' 
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.000000/3600 >= 1 then 1 
else 0 end as is_qualified_kpi  
,base1.delivery_cost
,base1.bonus
,base1.bwf
,base1.total_rating
FROM
(SELECT base.report_date
,base.created_year_week
,base.city_group
,base.uid
,base.hub_type
,base.shipper_name
,count(case when base.is_inshift = 1 then base.ref_order_id else null end ) as total_order
,count(case when base.is_late_eta = 1 then base.ref_order_id else null end) as total_late
,sum(case when base.is_inshift = 1 then base.delivery_cost else null end) as delivery_cost
,sum(case when base.is_inshift = 1 then base.bonus else null end) as bonus
,sum(case when base.is_inshift = 1 then base.bwf else null end) as bwf
,sum(rating) as total_rating
,count(case when rating is not null then base.ref_order_id else null end) as total_order_rating
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
 		WHEN st.shift_hour = 3 then '3 hour shift'
        WHEN st.shift_hour > 10 then 'All day shift'
        ELSE add.hub_type END AS hub_type
        ,case when date(from_unixtime(dot.real_drop_time -3600)) between date('2021-07-09') and date('2021-10-05') and psm.shipper_type_id = 12 and dot.pick_city_id = 217 then 1
        when date(from_unixtime(dot.real_drop_time -3600)) between date('2021-07-24') and date('2021-10-04') and psm.shipper_type_id = 12 and dot.pick_city_id = 218 then 1
        when cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_inshift
        ,dot.delivery_cost*1.00/100 as delivery_cost
        ,dot.ref_order_id
        ,case when date(from_unixtime(dot.real_drop_time -3600))  between date('2021-07-09') and date('2021-10-05') and dot.pick_city_id = 217 then 10000
        when date(from_unixtime(dot.real_drop_time -3600))  between date('2021-09-18') and date('2021-10-04') and dot.pick_city_id = 218 then 10000
        else 0 end as bonus
        ,coalesce(cast(json_extract(order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as bigint ),0) as bwf
        ,case when real_drop_time > estimated_drop_time then 1
              else 0 end as is_late_eta
        ,shipper_rate as rating

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

LEFT JOIN ( SELECT *,case
        WHEN shift_hour = 5 then '5 hour shift'
        WHEN shift_hour = 8 then '8 hour shift'
        WHEN shift_hour = 10 then '10 hour shift'
        WHEN shift_hour = 3 then '3 hour shift'   
        ELSE null END AS hub_type
        FROM
(select
    id,date_format(from_unixtime(start_time - 25200),'%H') as start_time
,date_diff('hour',date_trunc('hour',from_unixtime(start_time - 3600)),date_trunc('hour',from_unixtime(end_time-3600))) as shift_hour
FROM shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live)
) add on add.id = psm.shipper_shift_id

LEFT JOIN
(SELECT order_id
,shipper_uid as shipper_id
,case when cfo.shipper_rate = 0 then null
when cfo.shipper_rate = 1 or cfo.shipper_rate = 101 then 1
when cfo.shipper_rate = 2 or cfo.shipper_rate = 102 then 2
when cfo.shipper_rate = 3 or cfo.shipper_rate = 103 then 3
when cfo.shipper_rate = 104 then 4
when cfo.shipper_rate = 105 then 5
else null end as shipper_rate
,from_unixtime(cfo.create_time - 60*60) as create_ts

FROM shopeefood.foody_user_activity_db__customer_feedback_order_tab__reg_daily_s0_live cfo
)rate ON dot.ref_order_id = rate.order_id and dot.uid = rate.shipper_id


WHERE 1=1
AND dot.pick_city_id in (217,218,220)
AND psm.shipper_type_id in (12)
and dot.ref_order_category = 0
and psm.city_id in (217,218,220)
AND dot.order_status = 400
AND date(from_unixtime(dot.real_drop_time -3600)) >= date((current_date) - interval '60' day)
and date(from_unixtime(dot.real_drop_time -3600)) < date(current_date)
GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14)base
where base.is_inshift = 1
GROUP BY 1,2,3,4,5,6)base1
LEFT JOIN (select *
,case when first_day_in_hub between DATE('2021-01-01') and DATE('2021-01-03') then 202053
else YEAR(first_day_in_hub)*100 + WEEK(first_day_in_hub) end as week_join_hub
FROM hub
)hub on hub.shipper_id = base1.uid
left join (select * from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live where 1=1)hc
                                    on hc.uid = base1.uid and date(from_unixtime(hc.report_date - 3600)) = base1.report_date
                             
WHERE 1=1)base2

      
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub on hub.uid = base2.uid and date(from_unixtime(hub.report_date - 3600)) = base2.report_date
where 1 = 1 )
where is_resume = 1
)

,driver as
(
SELECT *

from
(SELECT shipper_id
,min(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) first_day_in_hub
--,max(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) last_day_in_hub
from shopeefood.foody_mart__profile_shipper_master

where 1=1
and shipper_type_id = 12 and grass_region = 'VN'
group by 1)

)
select

         a.shipper_id
        ,a.first_day_in_hub
        ,case when fresh.id is not null then 'Fresh' else 'Pt-16' end as source
        ,fresh.shift
        ,c.city_name
        ,c.shipper_name
        ,sum(case when b.report_date between a.first_day_in_hub and a.first_day_in_hub + interval '9' day then b.is_qualified_kpi else null end) as kpi
        ,sum(case when b.report_date between a.first_day_in_hub and a.first_day_in_hub + interval '9' day then b.total_order else null end) as total_order
        ,count(distinct case when b.report_date between a.first_day_in_hub and a.first_day_in_hub + interval '9' day then b.report_date else null end) as working_days


FROM driver a 
LEFT JOIN ado b on b.uid = a.shipper_id
LEFT JOIN vnfdbi_opsndrivers.foody_hub_fresh_driver_data fresh on cast(fresh.id as bigint) = a.shipper_id
LEFT JOIN shopeefood.foody_mart__profile_shipper_master c on c.shipper_id = a.shipper_id 
---



where   1 = 1
and a.first_day_in_hub between current_date - interval '30' day and current_date - interval '1' day
and c.grass_date = 'current'

group by 1, 2, 3, 4, 5, 6





