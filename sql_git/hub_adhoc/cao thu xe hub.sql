        with raw as
        (SELECT base.report_date
        ,base.created_year_week
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
                WHEN st.shift_hour = 5 then '5 hour shift'
                WHEN st.shift_hour = 8 then '8 hour shift'
                WHEN st.shift_hour = 10 then '10 hour shift'
                WHEN st.shift_hour = 3 then '3 hour shift'
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


        --- weekly bonus 
        SELECT   
                a.uid
                ,sm.shipper_name
                ,case when sm.city_name = 'Hai Phong City' and shift.hub_type = '5 hour shift' and start_shift < 15 then concat(shift.hub_type,' - S')
                when sm.city_name = 'Hai Phong City' and shift.hub_type = '5 hour shift' and start_shift >= 15 then concat(shift.hub_type,' - C')
                else shift.hub_type end as hub_type  
                ,sm.city_name
                ,reg.total_reg
                ,sum(a.total_order) as total_order 
                --,sum(case when (a.report_date = date'2022-04-03' or a.report_date = date'2022-04-10') then a.total_order else null end) as is_work_sun
                ,sum(a.is_qualified_kpi) as total_kpi
                ,count(distinct a.report_date) as working_day

        from ado a
        LEFT JOIN 
        (SELECT  uid 
                ,count(distinct case when registration_status != 'OFF' then date_ else null end ) as total_reg
                

        FROM
        (SELECT 
        date(from_unixtime(date_ts - 3600)) as date_
        ,uid
        ,case when registration_status = 1 then 'Registered'
        when registration_status = 2 then 'OFF'
        when registration_status = 3 then 'Worked'
        end as registration_status
        ,(end_time - start_time)/3600 as shift_hour
        ,slot_id
        FROM shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live
        ) 

        where date_ between date'2022-09-01' and date'2022-09-30'
        GROUP BY 1
        )reg on reg.uid = a.uid 

        ---Shift Final
        LEFT JOIN
        (   select   uid
                ,hub_type
                ,num_of_shift
                ,start_shift
                ,end_shift 
                ,row_number() over(partition by uid order by num_of_shift desc ) as rank 

        from 
        (SELECT 
                uid
                ,cast(json_extract(extra_data,'$.shift_category_name') as varchar) as hub_type
                ,hour(from_unixtime(cast(json_extract(extra_data,'$.shift_time_range[0]') as bigint) - 3600)) as start_shift
                ,hour(from_unixtime(cast(json_extract(extra_data,'$.shift_time_range[1]') as bigint) - 3600)) as end_shift
                ,count(distinct report_date) as num_of_shift 


        FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live 

        where date(from_unixtime(report_date - 3600)) between date'2022-09-01' and date'2022-09-30'
        -- and uid = 12119757
        group by 1,2,3,4
        )
        )
        shift on shift.uid = a.uid and shift.rank = 1 

        --Shipper Info 

        LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.uid and sm.grass_date = 'current'


        where 1 = 1 

        and a.report_date between date'2022-09-01' and date'2022-09-30'

        GROUP BY 1,2,3,4,5 







