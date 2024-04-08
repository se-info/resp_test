with raw as
(SELECT base.report_date
,case when base.report_date = date'2023-01-01' then 202252 else base.created_year_week end as created_year_week
,base.day_of_week
,base.city_group
,base.uid
,base.hub_type
,base.shift_calculated
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
        WHEN st.shift_hour/3600 = 5 then '5 hour shift'
        WHEN st.shift_hour/3600 = 8 then '8 hour shift'
        WHEN st.shift_hour/3600 = 10 then '10 hour shift'
        WHEN st.shift_hour/3600 = 3 then '3 hour shift'
        ELSE add.hub_type END AS hub_type
        ,st.shift_hour/cast(60 as double) as shift_calculated
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
,date_diff('second',from_unixtime(start_time),from_unixtime(end_time)) as shift_hour
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

-- and dot.ref_order_category = 0

and psm.city_id in (217,218,220)

AND dot.order_status = 400

GROUP by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16 

) base

where base.is_inshift = 1

GROUP BY 1,2,3,4,5,6,7,8

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
,base1.shift_calculated
,case --WHEN base1.hub_type = '5 hour shift' AND base1.total_order < 16 THEN 1
WHEN base1.hub_type = '8 hour shift' AND base1.total_order < 25 THEN 1
WHEN base1.hub_type = '10 hour shift' AND base1.total_order < 30 THEN 1
ELSE 0 END as less_than_min
,case --WHEN base1.hub_type = '5 hour shift' AND base1.total_order > 16 THEN 1
WHEN base1.hub_type = '8 hour shift' AND base1.total_order > 25 THEN 1
WHEN base1.hub_type = '10 hour shift' AND base1.total_order >30 THEN 1
ELSE 0 END AS over_min
,case --WHEN base1.hub_type = '5 hour shift' AND base1.total_order = 16 THEN 1
WHEN base1.hub_type = '8 hour shift' AND base1.total_order = 25 THEN 1
WHEN base1.hub_type = '10 hour shift' AND base1.total_order = 30 THEN 1
ELSE 0 END AS pass_min

--- KPI
,case when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '10 hour shift'
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/base1.shift_calculated >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 2 then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '8 hour shift'
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/base1.shift_calculated >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 2 then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '5 hour shift' and HOUR(from_unixtime(cast(json_extract(hub.extra_data,'$.shift_time_range[0]') as bigint) - 3600)) <> 6
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/base1.shift_calculated >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 1 then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '5 hour shift' and HOUR(from_unixtime(cast(json_extract(hub.extra_data,'$.shift_time_range[0]') as bigint) - 3600)) = 6
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/base1.shift_calculated >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
-- and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 1 
then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HCM'
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/base1.shift_calculated >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' then 1 
--and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 1 then 1 

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HN'
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/base1.shift_calculated >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' 
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)/cast(3600 as double) >= 1 then 1 

else 0 end as is_qualified_kpi

,(cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(3600 as double)) as online_time
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
,case when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '5 hour shift' 
            and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/cast(300 as double) >= 0.9 then 1 
      when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '10 hour shift' 
            and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/cast(600 as double) >= 0.9 then 1
      when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '8 hour shift' 
            and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/cast(485 as double) >= 0.9 then 1

      else 0 end as is_qualified_online_time                                    
,(cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)/cast(60 as double))/base1.shift_calculated as check_1
FROM raw base1

LEFT JOIN (
    select   *
            ,case when first_day_in_hub between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                  when first_day_in_hub = date'2023-01-01' then 202252  
            else YEAR(first_day_in_hub)*100 + WEEK(first_day_in_hub) end as week_join_hub
            FROM hub    
          )hs on hs.shipper_id = base1.uid

left join shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub on hub.uid = base1.uid 
                                                                                             and date(from_unixtime(hub.report_date - 3600)) = base1.report_date

)
)
-- select * from ado a  where a.report_date between date_trunc('week',current_date)- interval '7' day and date_trunc('week',current_date) - interval '1' day 
--                 and a.uid = 40002518



--- weekly bonus 

select   
         created_year_week
        ,uid
        ,shipper_name
        ,city_group
        ,hub_type
        -- ,case when is_work_sun > 0 then 1 else 0 end as is_work_sun
        ,case 
            --   Old logic  
            --   when created_year_week < 202222 and total_reg >= 4 
            --                                   and is_work_sun > 0 and kpi_sun = 1
            --                                   and total_reg = total_kpi
            --                                   and total_reg = working_day 
            --                                   then 50000      
            --   New logic updated from 05/06/2022
              when created_year_week >= 202222 and created_year_week <= 202237
                                              and is_work_sun > 0 and kpi_sun = 1
                                              and total_kpi >= 4 
                                              and total_reg = working_day then 50000
              when created_year_week > 202237 and hub_type in ('8 hour shift', '10 hour shift') and  kpi_sun = 1 then 50000
              when created_year_week > 202237 and hub_type = '5 hour shift' and  kpi_sun = 1 then 30000
              when created_year_week > 202237 and hub_type = '3 hour shift' and  kpi_sun = 1 then 20000    
                                              else 0 end as sunday_bonus

                          
        -- ,case when total_reg >= 4 and total_reg = total_kpi 
        --                           and total_reg = working_day then 1 else 0 end as qualified_kpi
        -- ,case when total_reg >= 6 and city_group in ('HCM','HN')
        --       and  working_day = total_kpi 
        --       and  working_day = total_reg then 300000
        --       when total_reg = 5 and city_group in ('HCM','HN')
        --       and  working_day = total_kpi 
        --       and  working_day = total_reg then 150000 
        --       when total_reg = 4 and city_group in ('HCM','HN')
        --       and  working_day = total_kpi 
        --       and  working_day = total_reg then 100000
        --       when total_kpi >= 6 and city_group = 'HP' then 200000
        --       when total_kpi >= 4 and city_group = 'HP' then 80000
        --       else 0 end as weekly_bonus_current
            --   estimate for change conditions
        ,case when total_kpi >= 6 and city_group in ('HCM','HN')
              and  working_day = total_reg 
            --   and  working_day = is_qualified_online_time 
              then 300000
              when total_kpi = 5 and city_group in ('HCM','HN')
              and  working_day = total_reg 
            --   and  working_day = is_qualified_online_time
              then 150000 
              when total_kpi = 4 and city_group in ('HCM','HN')
              and  working_day = total_reg 
            --   and  working_day = is_qualified_online_time 
              then 100000
              when total_kpi >= 6 and city_group = 'HP' then 200000
              when total_kpi >= 4 and city_group = 'HP' then 80000
              else 0 end as weekly_bonus_value
        ,coalesce(act.value,0) as weekly_bonus_paid
        ,coalesce(act_s.value,0) as sunday_bonus_paid                    
        ,working_day
        ,total_reg 
        ,total_kpi
        -- ,ado_ as ado
        -- ,total_ado
        -- ,is_qualified_online_time as total_time_qualified_online_time                              



from 
(SELECT   
         a.created_year_week
        ,a.uid
        ,a.shipper_name
        ,city_group
        ,reg.total_reg
        ,array_join(array_agg(distinct a.hub_type),',') as hub_type 
        ,sum(case when date_format(a.report_date,'%a') = 'Sun' then a.is_qualified_kpi else null end) as kpi_sun
        ,sum(case when date_format(a.report_date,'%a') = 'Sun' then a.total_order else null end) as is_work_sun
        ,sum(case when hub_type = '3 hour shift' then 0 else a.is_qualified_kpi end) as total_kpi
        ,count(distinct a.report_date) as working_day
        ,sum(total_order)/cast(count(distinct report_date) as double) as ado_   
        ,sum(total_order) as total_ado
        ,sum(is_qualified_online_time) as is_qualified_online_time

from ado a

LEFT JOIN 
(SELECT  created_year_week
        ,uid 
        ,count(distinct case when registration_status != 'OFF' then date_ else null end ) as total_reg
        
FROM
    (SELECT 
 date(from_unixtime(date_ts - 3600)) as date_
,case when date(from_unixtime(date_ts - 3600)) = date'2023-01-01' then 202252 
      else year(date(from_unixtime(date_ts - 3600)))*100+week(date(from_unixtime(date_ts - 3600))) 
      end as created_year_week 
,uid
,case when registration_status = 1 then 'Registered'
      when registration_status = 2 then 'OFF'
      when registration_status = 3 then 'Worked'
      end as registration_status
,(end_time - start_time)/3600 as shift_hour
,slot_id
FROM shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live

where date(from_unixtime(date_ts - 3600)) >= date'2022-05-01' 

    )

GROUP BY 1,2
)reg on reg.uid = a.uid and reg.created_year_week = a.created_year_week  

where a.report_date between date_trunc('week',current_date)- interval '7' day and date_trunc('week',current_date) - interval '1' day 

-- and a.hub_type != '3 hour shift'

GROUP BY 1,2,3,4,5

) a

LEFT JOIN 
        (select 
                  user_id
                 ,note
                 ,sum(balance/cast(100 as double)) as value    



        from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live

        where 1 = 1  
        -- user_id = 23127140
        -- and date(from_unixtime(create_time - 3600)) = date'2022-09-19'
        and note like '%HUB_MODEL_Thuong tai xe guong mau tuan 19/12 - 25/12%'
        group by 1,2
        ) act on act.user_id = a.uid 

LEFT JOIN 
        (select 
                  user_id
                 ,note
                 ,sum(balance/cast(100 as double)) as value    



        from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live

        where 1 = 1  
        -- user_id = 23127140
        -- and date(from_unixtime(create_time - 3600)) = date'2022-09-19'
        and note like '%HUB_MODEL_Thuong tai xe guong mau chu nhat tuan 19/12 - 25/12%'
        group by 1,2
        ) act_s on act_s.user_id = a.uid 

where 1 = 1
