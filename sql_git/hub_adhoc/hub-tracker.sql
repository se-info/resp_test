with ado as
(
SELECT uid 
      ,hub_type
    --   ,date_type
    ,'All days' as date_type  
        ,count(distinct report_date) as working_days
        ,sum(is_qualified_kpi) as kpi
        ,sum(total_rating)*1.0000/sum(total_order_rating) as rating
        ,sum(total_income)*1.0000 as total_income      

FROM
(SELECT
base2.report_date
,date_format(base2.report_date,'%a') as days_of_week
,case when date_format(base2.report_date,'%a') in ('Sun','Sat') then 'Weekends'
                                   when date_format(base2.report_date,'%a') not in ('Sun','Sat') then 'Weekdays' else null end date_type
,case when base2.report_date between date('2021-11-10') and date('2021-11-16') then 202101
              when base2.report_date between date('2021-11-03') and date('2021-11-09') then 202102
              else base2.created_year_week end as created_year_week
,base2.created_year_week as week_
,base2.uid
,base2.first_day_in_hub
,base2.city_group
,base2.hub_type
,base2.shipper_name
,base2.total_order as total_order_delivery
,base2.total_order_ship
,base2.total_late
,base2.total_late_ship
,base2.total_rating
,base2.total_order_rating
,base2.is_qualified_kpi
,cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT) as online_
,case when base2.report_date between date('2021-07-09') and date('2021-10-05') and base2.city_group = 'HCM' then base2.delivery_cost
when base2.report_date between date('2021-07-24') and date('2021-10-04') and base2.city_group = 'HN' then base2.delivery_cost
else coalesce(cast(json_extract(hc.extra_data,'$.calculated_shipping_shared') as bigint),base2.total_order*13500) end as ship_shared

,case when base2.report_date between date('2021-07-09') and date('2021-10-05') and base2.city_group = 'HCM' then base2.bonus
when base2.report_date between date('2021-07-24') and date('2021-10-04') and base2.city_group = 'HN' then base2.bonus
else coalesce(cast(json_extract(hc.extra_data,'$.total_bonus') as bigint),0) end as daily_bonus


,case when cast(json_extract(hc.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then (cast(json_extract(hc.extra_data,'$.total_income') as bigint) - cast(json_extract(hc.extra_data,'$.calculated_shipping_shared') as bigint))
else 0 end as extra_ship

,base2.bwf
,0 as surge

,case when base2.report_date between date('2021-07-09') and date('2021-10-05') and base2.city_group = 'HCM' then base2.delivery_cost + base2.bonus
when base2.report_date between date('2021-07-24') and date('2021-10-04') and base2.city_group = 'HN' then base2.delivery_cost + base2.bonus
else if(cast(json_extract(hc.extra_data,'$.total_order') as bigint) = base2.total_order,cast(json_extract(hc.extra_data,'$.total_income') as bigint),13500*base2.total_order) end as total_income
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
,base1.total_order_ship
,base1.total_late
,base1.total_late_ship
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

--- KPI
,case when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '10 hour shift'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/600 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.00000000/3600 >= 2 then 1
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '8 hour shift'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/485 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 2 then 1
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '5 hour shift'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/300 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 then 1
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HCM'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/180 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' then 1
--and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.000/3600 >= 1 then 1
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HN'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/180 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 then 1
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
,count(case when base.is_inshift = 1 and base.ref_order_category = 0 then base.ref_order_id else null end ) as total_order
,count(case when base.is_inshift = 1 and base.ref_order_category <> 0 then base.ref_order_id else null end ) as total_order_ship
,count(case when base.is_late_eta = 1 and base.ref_order_category = 0 then base.ref_order_id else null end) as total_late
,count(case when base.is_late_eta = 1 and base.ref_order_category <> 0 then base.ref_order_id else null end) as total_late_ship
,sum(case when base.is_inshift = 1 then base.delivery_cost else null end) as delivery_cost
,sum(case when base.is_inshift = 1 then base.bonus else null end) as bonus
,sum(case when base.is_inshift = 1 then base.bwf else null end) as bwf
,sum(rating) as total_rating
,count(case when rating is not null then base.ref_order_id else null end) as total_order_rating
FROM
(SELECT dot.uid
        ,psm.shipper_name
        ,dot.ref_order_category
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
AND date(from_unixtime(dot.real_drop_time -3600)) >= date((current_date) - interval '75' day)
and date(from_unixtime(dot.real_drop_time -3600)) < date(current_date)
GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15)base
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


LEFT JOIN shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc on hc.uid = base2.uid and date(from_unixtime(hc.report_date - 3600)) = base2.report_date
where 1 = 1 )
-- where is_resume = 1
where 1 = 1 
  and   report_date between  date'2022-04-01' and date'2022-04-30'
  and   report_date != date'2022-04-04'
  group by 1,2,3
)

,driver as
(
SELECT *

from
(SELECT shipper_id
,min(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) first_day_in_hub
,max(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) last_day_in_hub
from shopeefood.foody_mart__profile_shipper_master

where 1=1
and shipper_type_id = 12 and grass_region = 'VN'
group by 1)

)
select
        --  reg.date_
         reg.uid
        ,reg.date_type
        -- ,'All days' as date_type
        ,reg.shift_type      
        ,c.city_name
        ,c.shipper_name
        ,ps.gender
        ,2022 - cast(substr(cast(ps.birth_date as varchar),1,4) as bigint) as age_
        ,b.first_day_in_hub
        ,date(from_unixtime(ps.create_time - 3600)) as onboard_date
        ,date_diff('day',date(b.first_day_in_hub),date'2022-04-30') as seniority_in_hub
        -- ,online_*1.00000000/3600 as online_inshift
        ,kpi
        ,rating
        ,total_income
        ,working_days
        ,count(distinct case when registration_status is not null then date_ts else null end) as total_registered
        ,count(distinct case when registration_status = 2 then date_ts else null end) as total_cancelled_slot



from (select
                  slot.uid 
                --  ,(end_time - start_time)/3600 as shift_hour
                 ,concat(cast((end_time - start_time)/3600 as varchar ),' hour shift') as shift_type
                --  ,case when date_format(date(from_unixtime(date_ts - 3600)),'%a') in ('Sun','Sat') then 'Weekends'
                --        when date_format(date(from_unixtime(date_ts - 3600)),'%a') not in ('Sun','Sat') then 'Weekdays' else null end as date_type
                 ,'All days' as date_type      
                --  ,case when registration_status = 1 then 'Registered'
                --        when registration_status = 2 then 'OFF'
                --        when registration_status = 3 then 'Worked'
                --        end as registration_status
                ,registration_status
                ,date(from_unixtime(date_ts - 3600)) as date_ts


                --  ,count(date_ts) as total_reg 
         
        from shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live slot
         where date(from_unixtime(date_ts - 3600)) between date'2022-04-01' and date'2022-04-30'
         and date(from_unixtime(date_ts - 3600)) != date'2022-04-04'
        --  and uid = 1425088
        --  group by 1,2,3
         ) reg 

-- personal info
left join shopeefood.foody_internal_db__shipper_info_personal_tab__reg_continuous_s0_live ps on ps.uid = reg.uid

--ADO info

left join driver b on reg.uid = b.shipper_id
LEFT JOIN shopeefood.foody_mart__profile_shipper_master c on c.shipper_id = reg.uid and c.grass_date = 'current' 

left join ado a ON a.uid = reg.uid 
         
         and a.date_type = reg.date_type
         and a.hub_type = reg.shift_type 




--HUB locations
-- left join
-- (SELECT a1.date_
--       ,a1.uid as shipper_id
--       ,coalesce(concat(a1.hub_name_1,',',a2.hub_name_2,',',a3.hub_name_3),coalesce(a1.hub_name_1,a2.hub_name_2,a3.hub_name_3)) as hub_locations

-- FROM
-- (
-- select *,date(from_unixtime(hc.report_date - 3600)) as date_ ,info.hub_name as hub_name_1
-- from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc
-- LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
-- info on  info.id = cast(json_extract(hc.extra_data,'$.hub_ids[0]') as bigint)
-- )a1

-- LEFT JOIN
-- (select *,date(from_unixtime(hc.report_date - 3600)) as date_ ,info.hub_name as hub_name_2
-- from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc
-- LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
-- info on  info.id = cast(json_extract(hc.extra_data,'$.hub_ids[1]') as bigint)
-- )a2 on a2.date_ = a1.date_ and a2.uid = a1.uid

-- left  join
-- (select *,date(from_unixtime(hc.report_date - 3600)) as date_ ,info.hub_name as hub_name_3
-- from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc
-- LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
-- info on  info.id = cast(json_extract(hc.extra_data,'$.hub_ids[2]') as bigint)
-- )a3 on a3.date_ = a1.date_ and a3.uid = a1.uid

-- order by date_ desc
-- )hub on hub.date_ = reg.date_ and hub.shipper_id = reg.uid


  where   1 = 1

--   and   reg.uid = 1425088 


group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14

-- group by 1,2,3

