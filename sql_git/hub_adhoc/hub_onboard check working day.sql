with hub_onboard AS
(SELECT
      shipper_id
    , shipper_ranking - type_ranking AS groupx_
    , MIN(report_date) AS first_join_hub
    , MAX(report_date) AS last_drop_hub
FROM
    (SELECT
        shipper_id
        , shipper_type_id
        , DATE(grass_date) AS report_date
        , RANK() OVER (PARTITION BY shipper_id ORDER BY DATE(grass_date)) AS shipper_ranking
        , RANK() OVER (PARTITION BY shipper_id, shipper_type_id ORDER BY DATE(grass_date)) AS type_ranking
    FROM shopeefood.foody_mart__profile_shipper_master
    WHERE shipper_type_id IN (12, 11)
    AND grass_date != 'current'
    )
WHERE shipper_type_id = 12
GROUP BY 1,2
)
,raw as
(SELECT base.report_date
,base.created_year_week
,base.day_of_week
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
        WHEN dot.pick_city_id = 220 then 'HP'
        ELSE NULL end as city_group
        ,psm.city_name
        ,date(from_unixtime(dot.real_drop_time -3600)) as report_date
        ,date_format(from_unixtime(dot.real_drop_time -3600),'%a') as day_of_week
        ,YEAR(date(from_unixtime(dot.real_drop_time -3600)))*100 + WEEK(date(from_unixtime(dot.real_drop_time -3600))) as created_year_week
,case
        WHEN st.shift_hour = 5 then '5 hour shift'
        WHEN st.shift_hour = 8 then '8 hour shift'
        WHEN st.shift_hour = 10 then '10 hour shift'
        WHEN st.shift_hour = 3 then '3 hour shift'
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
        WHEN shift_hour = 3 then '3 hour shift'
        ELSE null END AS hub_type
        FROM
(select 
    id,date_format(from_unixtime(start_time - 25200),'%H') as start_time
,date_diff('hour',date_trunc('hour',from_unixtime(start_time - 3600)),date_trunc('hour',from_unixtime(end_time-3600))) as shift_hour
FROM shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live)
) add on add.id = psm.shipper_shift_id

---rating
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

and psm.city_id in (217,218,220)

AND dot.order_status = 400

GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

) base

where base.is_inshift = 1

GROUP BY 1,2,3,4,5,6,7

)
, hub AS
(SELECT shipper_id
,min(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) first_day_in_hub
,max(case when grass_date = 'current' then date(current_date) else cast(grass_date as date) end) last_day_in_hub
from shopeefood.foody_mart__profile_shipper_master

where 1=1
and shipper_type_id = 12 and grass_region = 'VN'
group by 1)

,ado as 
(
SELECT *

FROM
(
SELECT base1.report_date
,base1.created_year_week
,base1.day_of_week
,base1.uid
,base1.shipper_name
,hs.first_day_in_hub
,case when hs.week_join_hub < (base1.created_year_week - 1) then 'Current Driver'
when hs.week_join_hub >= (base1.created_year_week - 1) then 'New Driver'
else null end as hub_seniority
,base1.city_group
,cast(json_extract(hub.extra_data,'$.shift_category_name') as VARCHAR) as hub_type
,base1.total_order
,base1.total_late
,base1.total_order_rating
,case WHEN base1.hub_type = '5 hour shift' AND base1.total_order < 16 THEN 1
WHEN base1.hub_type = '8 hour shift' AND base1.total_order < 25 THEN 1
WHEN base1.hub_type = '10 hour shift' AND base1.total_order < 30 THEN 1
ELSE 0 END as less_than_min
,case WHEN base1.hub_type = '5 hour shift' AND base1.total_order > 16 THEN 1
WHEN base1.hub_type = '8 hour shift' AND base1.total_order > 25 THEN 1
WHEN base1.hub_type = '10 hour shift' AND base1.total_order >30 THEN 1
ELSE 0 END AS over_min
,case WHEN base1.hub_type = '5 hour shift' AND base1.total_order = 16 THEN 1
WHEN base1.hub_type = '8 hour shift' AND base1.total_order = 25 THEN 1
WHEN base1.hub_type = '10 hour shift' AND base1.total_order = 30 THEN 1
ELSE 0 END AS pass_min

--- KPI
,case when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '10 hour shift'
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/600 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.00000000/3600 >= 2 then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '8 hour shift'
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/485 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 2 then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '5 hour shift' and HOUR(from_unixtime(cast(json_extract(hub.extra_data,'$.shift_time_range[0]') as bigint) - 3600)) <> 6
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/300 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '5 hour shift' and HOUR(from_unixtime(cast(json_extract(hub.extra_data,'$.shift_time_range[0]') as bigint) - 3600)) = 6
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/300 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
-- and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 
then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HCM'
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/180 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' then 1 
--and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.000/3600 >= 1 then 1 

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HN'
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/180 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' 
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 then 1 

else 0 end as is_qualified_kpi

,(cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/3600) as online_time
,cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT) as online_ 
,case when base1.report_date between date('2021-07-09') and date('2021-10-05') and base1.city_group = 'HCM' then base1.delivery_cost
when base1.report_date between date('2021-07-24') and date('2021-10-04') and base1.city_group = 'HN' then base1.delivery_cost
else coalesce(cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint),base1.total_order*13500) end as ship_shared

,case when base1.report_date between date('2021-07-09') and date('2021-10-05') and base1.city_group = 'HCM' then base1.bonus 
when base1.report_date between date('2021-07-24') and date('2021-10-04') and base1.city_group = 'HN' then base1.bonus 
else coalesce(cast(json_extract(hub.extra_data,'$.total_bonus') as bigint),0) end as daily_bonus


,case when cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' then (cast(json_extract(hub.extra_data,'$.total_income') as bigint) - cast(json_extract(hub.extra_data,'$.calculated_shipping_shared') as bigint))
else 0 end as extra_ship

,case when cast(json_extract(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) = 'true' 
           and (case WHEN base1.hub_type = '5 hour shift' AND base1.total_order < 16 THEN 1
                     WHEN base1.hub_type = '8 hour shift' AND base1.total_order < 25 THEN 1
                     WHEN base1.hub_type = '10 hour shift' AND base1.total_order < 30 THEN 1
                     else 0 end) = 1
          then 1 
      else 0 end as is_compensated
,case when base1.report_date between date('2021-07-09') and date('2021-10-05') and base1.city_group = 'HCM' then base1.delivery_cost + base1.bonus 
when base1.report_date between date('2021-07-24') and date('2021-10-04') and base1.city_group = 'HN' then base1.delivery_cost + base1.bonus
else if(cast(json_extract(hub.extra_data,'$.total_order') as bigint) = base1.total_order,cast(json_extract(hub.extra_data,'$.total_income') as bigint),13500*base1.total_order) end as total_income

FROM raw base1

LEFT JOIN (
    select   *
            ,case when first_day_in_hub between DATE('2021-01-01') and DATE('2021-01-03') then 202053
            else YEAR(first_day_in_hub)*100 + WEEK(first_day_in_hub) end as week_join_hub
            FROM hub    
          )hs on hs.shipper_id = base1.uid

left join shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub on hub.uid = base1.uid 
                                                                                             and date(from_unixtime(hub.report_date - 3600)) = base1.report_date


)
)

select 
        a.*
       ,sm.city_name  
       ,current.current_driver_tier as tier_join_hub
       ,IF(sm.shipper_type_id = 12, 'Hub','Non Hub') as current_shipper_type 
       ,count(distinct ado.report_date) as working_day  




from hub_onboard a 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id and sm.grass_date = 'current'

inner join ado on ado.uid = a.shipper_id
               and ado.report_date between first_join_hub and last_drop_hub

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

        -- where cast(from_unixtime(bonus.report_date - 60*60) as date) = date(current_date) - interval '1' day
        )current on current.shipper_id = a.shipper_id and current.report_date = a.first_join_hub - interval '1' day

where 1 = 1 

and a.first_join_hub between current_date - interval '60' day and current_date - interval '1' day

and current.current_driver_tier in ('T3','T4','T5')

group by 1,2,3,4,5,6,7

-- having(count(distinct ado.report_date) <= 5)

-- select * from ado where uid = 2996387