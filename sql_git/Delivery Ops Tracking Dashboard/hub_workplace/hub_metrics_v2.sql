DROP TABLE IF EXISTS dev_vnfdbi_opsndrivers.phong_hub_driver_metrics;
create table if not exists  dev_vnfdbi_opsndrivers.phong_hub_driver_metrics as 
WITH assignment AS 
(SELECT 
        sa.order_id
       ,COALESCE(ogm.ref_order_id,dot.ref_order_id) AS ref_order_id 
       ,COALESCE(ogm.ref_order_code,dot.ref_order_code) AS order_code
       ,COALESCE(ogi.ref_order_category,sa.order_type) AS order_category
       ,sa.status
       ,sa.shipper_uid AS driver_id
       ,FROM_UNIXTIME(sa.create_time - 3600) AS create_time
       ,CASE 
            WHEN sa.assign_type = 1 then '1. Single Assign'
            WHEN sa.assign_type in (2,4) then '2. Multi Assign'
            WHEN sa.assign_type = 3 then '3. Well-Stack Assign'
            WHEN sa.assign_type = 5 then '4. Free Pick'
            WHEN sa.assign_type = 6 then '5. Manual'
            WHEN sa.assign_type in (7,8) then '6. New Stack Assign'
            ELSE NULL END AS assign_type
       ,CASE 
            WHEN sa.order_type = 200 then 'Group'
            ELSE 'Single' END AS order_type
       ,dot.order_status                      

FROM 
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live


        UNION
    
        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
) sa 

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogi
    on ogi.id = (CASE WHEN sa.order_type = 200 THEN sa.order_id ELSE 0 END)

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogm 
    on ogm.group_id = ogi.id
    and ogm.ref_order_category = ogi.ref_order_category
    and ogm.create_time <= sa.create_time

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = (CASE WHEN sa.order_type = 200 THEN ogm.ref_order_id ELSE sa.order_id END) 
    and dot.ref_order_category = (CASE WHEN sa.order_type = 200 THEN ogm.ref_order_category ELSE sa.order_type END)


WHERE 1 = 1
AND DATE(FROM_UNIXTIME(sa.create_time - 3600)) BETWEEN current_date - interval '90' day AND current_date - interval '1' day
AND sa.status in (3,4)
) 
,raw as 
(SELECT 
         dot.uid AS shipper_id 
        ,psm.shipper_name
        ,case
        WHEN dot.pick_city_id = 217 then 'HCM'
        WHEN dot.pick_city_id = 218 then 'HN'
        WHEN dot.pick_city_id = 220 then 'HP'
        ELSE 'OTH' end as city_group
        ,psm.city_name
        ,date(from_unixtime(dot.real_drop_time -3600)) as report_date
        ,YEAR(date(from_unixtime(dot.real_drop_time -3600)))*100 + WEEK(date(from_unixtime(dot.real_drop_time -3600))) as created_year_week
        ,case 
              when date(from_unixtime(dot.real_drop_time -3600)) between date('2021-07-09') and date('2021-10-05') and psm.shipper_type_id = 12 and dot.pick_city_id = 217 then 1
              when date(from_unixtime(dot.real_drop_time -3600)) between date('2021-07-24') and date('2021-10-04') and psm.shipper_type_id = 12 and dot.pick_city_id = 218 then 1
              when cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_inshift
        ,dot.delivery_cost/CAST(100 AS DOUBLE) as delivery_cost
        ,dot.ref_order_id
        ,dot.ref_order_code
        ,dot.ref_order_category
        ,CASE 
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 1 THEN '5 hour shift'
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 2 THEN '8 hour shift'
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 3 THEN '10 hour shift'
             WHEN CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS INT) = 4 THEN '3 hour shift'
             ELSE 'Non Hub' end as hub_type        
        ,CASE WHEN date(from_unixtime(dot.real_drop_time -3600))  between date('2021-07-09') and date('2021-10-05') and dot.pick_city_id = 217 then 10000
        when date(from_unixtime(dot.real_drop_time -3600))  between date('2021-09-18') and date('2021-10-04') and dot.pick_city_id = 218 then 10000
        else 0 end as bonus 
        ,coalesce(cast(json_extract(order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as bigint ),0) as bwf
        ,rate.shipper_rate as rating
        ,CASE WHEN real_drop_time > estimated_drop_time then 1 
              else 0 end as is_late_eta
        ,DATE(sa.create_time) AS inflow_date              
        ,sa.create_time AS last_incharge_timestamp                             

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN assignment sa 
    on sa.ref_order_id = dot.ref_order_id
    and sa.order_category = dot.ref_order_category

LEFT JOIN assignment sa_filter
    on  sa.ref_order_id = sa_filter.ref_order_id          
    and sa.order_category = sa_filter.order_category 
    and sa.create_time < sa_filter.create_time

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet on dot.id = doet.order_id

LEFT JOIN shopeefood.foody_mart__profile_shipper_master psm 
    ON psm.shipper_id = dot.uid and TRY_CAST(psm.grass_date AS DATE) = date(from_unixtime(dot.real_drop_time -3600))

left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on oct.id = dot.ref_order_id

         
LEFT JOIN
    (SELECT order_id
    ,shipper_uid as shipper_id
    ,CASE WHEN cfo.shipper_rate = 0 then null
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
AND sa_filter.order_id is null
AND dot.order_status = 400
AND psm.shipper_type_id = 12
AND cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) = 2
AND date(sa.create_time) BETWEEN current_date - interval '90' day and current_date - interval '1' day

)
,metrics AS 
(SELECT  
         raw.shipper_id 
        ,raw.inflow_date
        ,CAST(json_extract(hub.extra_data,'$.shift_category_name') AS VARCHAR) AS hub_type
        ,CAST(json_extract(hub.extra_data,'$.shift_category_name') AS VARCHAR)
            ||'-'||
        CAST(HOUR(FROM_UNIXTIME(CAST(CAST(json_extract(extra_data,'$.shift_time_range') AS array(json))[1] AS bigint) - 3600)) AS VARCHAR) AS hub_type_x_start_time
        ,FROM_UNIXTIME(CAST(CAST(json_extract(extra_data,'$.shift_time_range') AS array(json))[1] AS bigint) - 3600) AS start_shift_time
        ,FROM_UNIXTIME(CAST(CAST(json_extract(extra_data,'$.shift_time_range') AS array(json))[2] AS bigint) - 3600) AS end_shift_time
        ,raw.city_group
        ,hub.slot_id
        ,CAST(json_extract_scalar(hub.extra_data,'$.is_apply_fixed_amount') as VARCHAR) as is_compensate
        ,CASE WHEN cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '10 hour shift'
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
                                                                                                           
        then 1

        when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HCM'   
        and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
        and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
        and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/180 >= 0.9
        and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' then 1 
                                                                                                             

        when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HN'
        and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
        and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
        and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/180 >= 0.9
        and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' 
        and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 then 1 
        ELSE 0 END AS is_qualified_kpi
        
        ,CAST(json_extract(hub.extra_data,'$.calculated_shipping_shared') AS BIGINT) AS ship_shared
        ,COALESCE(CAST(json_extract(hub.extra_data,'$.total_bonus') AS BIGINT),0) AS daily_bonus
        ,CASE 
             WHEN CAST(json_extract(hub.extra_data,'$.is_apply_fixed_amount') AS VARCHAR) = 'true' 
                  THEN (CAST(json_extract(hub.extra_data,'$.total_income') AS BIGINT) - CAST(json_extract(hub.extra_data,'$.calculated_shipping_shared') AS BIGINT))
             ELSE 0 END AS extra_ship
        ,CAST(json_extract(hub.extra_data,'$.total_income') AS BIGINT) AS total_income

        ,COUNT(DISTINCT (raw.shipper_id,raw.ref_order_id)) AS total_order
        ,COUNT(DISTINCT CASE WHEN raw.ref_order_category = 0 THEN (shipper_id,ref_order_id) ELSE NULL END) AS total_order_delivery
        ,COUNT(DISTINCT CASE WHEN raw.ref_order_category != 0 THEN (shipper_id,ref_order_id) ELSE NULL END) AS total_order_spxi
        ,COUNT(DISTINCT CASE WHEN raw.is_late_eta = 1 THEN (shipper_id,ref_order_id) ELSE NULL END) AS total_late
        ,SUM(raw.rating) AS total_rating 
        ,COUNT(DISTINCT CASE WHEN raw.rating IS NOT NULL THEN (shipper_id,ref_order_id) ELSE NULL END) AS total_order_rating

FROM raw 

LEFT JOIN (select * from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live ) hub 
    on hub.uid = raw.shipper_id 
       and DATE(from_unixtime(hub.report_date - 3600)) = raw.inflow_date
    --    and cast(json_extract(hub.extra_data,'$.shift_category_name') AS VARCHAR) = raw.hub_type
       and raw.last_incharge_timestamp 
       between 
       FROM_UNIXTIME(CAST(CAST(json_extract(extra_data,'$.shift_time_range') AS array(json))[1] AS bigint) - 3600)
       and 
       FROM_UNIXTIME(CAST(CAST(json_extract(extra_data,'$.shift_time_range') AS array(json))[2] AS bigint) - 3600)
                                                                                               
WHERE 1 = 1 

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
,online_performance as 
(SELECT
        report_date
       ,shipper_id
       ,hub_type
       ,slot_id
       ,sum(total_online_time) as total_online_time
       ,sum(total_working_time) as total_working_time
       ,sum(in_shift_online_time) as in_shift_online_time
       ,sum(in_shift_work_time) as in_shift_work_time 
FROM
(SELECT 
        base.report_date 
        ,base.shipper_id
        ,base.hub_type
        ,base.slot_id
                
        ,date_diff('second',base.actual_start_time_online,base.actual_end_time_online)*1.0000/(60*60) as total_online_time
        ,date_diff('second',base.actual_start_time_work,base.actual_end_time_work)*1.0000/(60*60) as total_working_time
                 
        ,case when base.actual_end_time_online < base.start_shift_time then 0
            when base.actual_start_time_online > base.end_shift_time then 0
            else date_diff('second',   greatest(base.start_shift_time,base.actual_start_time_online)   ,   least(base.end_shift_time,base.actual_end_time_online)   )*1.0000/(60*60)
            end as in_shift_online_time
        ,case when base.actual_end_time_work < base.start_shift_time then 0
            when base.actual_start_time_work > base.end_shift_time then 0
            else date_diff('second',   greatest(base.start_shift_time,base.actual_start_time_work)   ,   least(base.end_shift_time,base.actual_end_time_work)   )*1.0000/(60*60)
            end as in_shift_work_time

FROM
(SELECT 
         sts.uid as shipper_id
        ,shift.slot_id 
        ,date(from_unixtime(create_time - 60*60)) as report_date
        ,CASE WHEN shift.shift_hour = 10 then '10 hour shift'
        WHEN shift.shift_hour = 8 then '8 hour shift'
        WHEN shift.shift_hour = 5 then '5 hour shift'
        WHEN shift.shift_hour = 3 then '3 hour shift'
        ELSE NULL END as hub_type
                              
        ,from_unixtime(check_in_time - 60*60) as check_in_time
        ,from_unixtime(check_out_time - 60*60) as check_out_time
        ,from_unixtime(order_start_time - 60*60) as order_start_time
        ,from_unixtime(order_end_time - 60*60) as order_end_time

                       
        ,check_in_time as check_in_time_original
        ,check_out_time as check_out_time_original
        ,order_start_time as order_start_time_original
        ,order_end_time as order_end_time_original
                          

        ,total_online_seconds*1.00/(60*60) as total_online_hours
        ,(check_out_time - check_in_time)*1.00/(60*60) as online
        ,total_work_seconds*1.00/(60*60) as total_work_hours
        ,(order_end_time - order_start_time)*1.00/(60*60) as work

                     
        ,from_unixtime(check_in_time - 60*60) as actual_start_time_online
        ,greatest(from_unixtime(check_out_time - 60*60),from_unixtime(order_end_time - 60*60)) as actual_end_time_online

        ,case when order_start_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_start_time - 60*60) end as actual_start_time_work
        ,case when order_end_time = 0 then from_unixtime(check_in_time - 60*60) else from_unixtime(order_end_time - 60*60) end as actual_end_time_work
        ,date_add('hour',shift.start_shift,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP)) as start_shift_time
        ,date_add('hour',shift.end_shift,cast(date(from_unixtime(check_in_time - 60*60)) as TIMESTAMP)) as end_shift_time

from shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live sts

left join 
(
SELECT 
         date(from_unixtime(date_ts - 3600)) as date_
        ,uid
        ,slot_id
        ,case when registration_status = 1 then 'Registered'
            when registration_status = 2 then 'OFF'
            when registration_status = 3 then 'Worked'
            end as registration_status
        ,(end_time - start_time)/3600 as shift_hour
        ,start_time/3600 as start_shift
        ,end_time/3600 as end_shift

from shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live
where registration_status != 2 
)shift on shift.uid = sts.uid and shift.date_ = date(from_unixtime(create_time - 60*60))


)base
where hub_type is not null
and report_date >= current_date - interval '90' day
)
group by 1,2,3,4
)
,kpi_qualified AS
(SELECT
      hub.uid AS shipper_id
    , slot_id  
    , DATE(FROM_UNIXTIME(hub.report_date - 3600)) AS report_date

    , CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) AS hub_shift

    , CAST(json_extract(hub.extra_data,'$.stats.deny_count') AS BIGINT) AS deny_count
    , CAST(json_extract(hub.extra_data,'$.stats.ignore_count') AS BIGINT) AS ignore_count

    , CAST(json_extract(hub.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 AS online_in_shift
    , CAST(json_extract(hub.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 AS online_peak_hour

    , regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') AS is_auto_accept

    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600) start_shift
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600) end_shift
    ,date_diff('second',from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
    , from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/3600.00 as time_in_shift


from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub 
)
,kpi as 
(select uid 
       ,CAST(json_extract(extra_data,'$.shift_category_name') AS varchar) as hub_shift 
       ,cast(json_extract(extra_data,'$.passed_conditions') as bigint) as passed_conditions
       ,date(from_unixtime(report_date - 3600)) as report_date
       ,case when t.test_1 = 1 then 'Online in shift'
             when t.test_1 = 2 then 'Online peak hour'
             when t.test_1 = 3 then 'Denied'
             when t.test_1 = 4 then 'Ignore'
             when t.test_1 = 6 then 'Auto Accept'
             when t.test_1 = 5 then 'Min service level rate'
             when t.test_1 = 7 then 'Non checkout bad weather'            
            else null end as passed_conditions__v2  
        ,extra_data
        ,slot_id     




from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live 

cross join unnest 
(
cast(json_extract(extra_data,'$.passed_conditions') as array<int>)
) t(test_1)

)

,cond(compare_condition) as(
VALUES
(array['Min service level rate','Auto Accept','Ignore','Denied','Online peak hour','Online in shift'])
)

,kpi_v2 as 
(select     * 
            ,array_except(cond.compare_condition,conditions_pass) as kpi_failed        

from
(select 
        uid
       ,report_date
       ,hub_shift
       ,extra_data
       ,slot_id
       ,array_agg(passed_conditions__v2) as conditions_pass

       from kpi 
       

where report_date between current_date - interval '90' day and current_date - interval '1' day

group by 1,2,3,4,5
)

cross join cond 
                   
)
,hub_onboard AS
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
,hub_locations AS 
(SELECT 
        hub.uid
       ,date(from_unixtime(hub.date_ts - 3600)) as date_ 
       ,cf.hub_id
       ,inf.hub_name AS hub_locations
       ,hub.slot_id

FROM shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live hub

LEFT JOIN shopeefood.foody_internal_db__shipper_config_slot_tab__reg_daily_s0_live cf 
    on cf.id = hub.slot_id

LEFT JOIN shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live inf 
    on inf.id = cf.hub_id
)

SELECT
       reg.date_
      ,reg.slot_id  
      ,reg.uid
      ,pf.shopee_uid
      ,sm.city_name  
      ,sm.shipper_name  
      ,ho.first_join_hub AS first_day_in_hub
      ,DATE_DIFF('day',DATE(ho.first_join_hub),reg.date_) AS duration_
      ,reg.shift_hour AS hub_type_original
      ,reg.hub_type_x_start_time
      ,CASE 
            WHEN reg.registration_status = 'OFF' then 0
            ELSE 1 END AS registered_
      ,registration_status               
      ,hub_locations AS hub_locations
      ,ARRAY_JOIN(k2.kpi_failed,',') AS kpi_failed             
      ,op.in_shift_online_time
      ,op.in_shift_work_time
      ,(op.in_shift_online_time - op.in_shift_work_time)/cast(op.in_shift_online_time as double) as down_time_rate
      ,k1.online_peak_hour
      ,k1.is_auto_accept
      ,k1.deny_count
      ,k1.ignore_count
      ,CASE WHEN TRY(CARDINALITY(FILTER(k2.kpi_failed,x -> x is not null))) <= 0 THEN 1 ELSE 0 END AS kpi 
      ,a.start_shift_time
      ,a.end_shift_time 
      ,try(sum(a.total_order)*1.0000/count(distinct a.inflow_date)) as total_order
      ,try(sum(a.total_order_delivery)*1.0000/count(distinct a.inflow_date)) as total_order_delivery
      ,try(sum(a.total_order_spxi)*1.0000/count(distinct a.inflow_date)) as total_order_spxi
                                                                                                 
      ,try(sum(a.total_late)*1.0000/sum(a.total_order)) as late_rate
                                                                                        
      ,try(sum(a.total_rating)*1.0000/sum(a.total_order_rating)) as rating
      ,try(sum(a.total_income)*1.0000/count(distinct a.inflow_date)) as total_income
      ,try(sum(a.extra_ship)*1.0000/count(distinct a.inflow_date)) as extra_ship
      ,try(sum(a.daily_bonus)*1.0000/count(distinct a.inflow_date)) as daily_bonus        
FROM
    (
        SELECT
        date(from_unixtime(date_ts - 3600)) as date_
        ,uid
        ,case when registration_status = 1 then 'Registered'
            when registration_status = 2 then 'OFF'
            when registration_status = 3 then 'Worked'
            end as registration_status
        ,CAST(((end_time - start_time)/3600) AS VARCHAR) ||' '||'hour shift' as shift_hour
        ,CAST(((end_time - start_time)/3600) AS VARCHAR) ||' '||'hour shift'||'-'||CAST((start_time/3600) AS VARCHAR) AS hub_type_x_start_time
        ,start_time/3600 as start_shift
        ,slot_id
        FROM shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live
    ) reg   

LEFT JOIN shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live pf 
    on pf.uid = reg.uid

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm
    on sm.shipper_id = reg.uid and try_cast(sm.grass_date AS DATE) = reg.date_    

LEFT JOIN (SELECT 
                  *
                  ,row_number()over(partition by shipper_id order by first_join_hub desc) AS rank_    
            FROM hub_onboard) ho 
    on ho.shipper_id = reg.uid and ho.rank_ = 1 

LEFT JOIN hub_locations hl 
    on hl.uid = reg.uid and reg.date_ = hl.date_ 
    and hl.slot_id = (CASE 
                          WHEN hl.slot_id > 0 THEN reg.slot_id ELSE 0 END)

LEFT JOIN online_performance op 
    on op.shipper_id = reg.uid and reg.date_ = op.report_date and reg.slot_id = op.slot_id

LEFT JOIN kpi_qualified k1 
    on k1.shipper_id = reg.uid 
    and k1.slot_id = (CASE 
                          WHEN k1.slot_id > 0 THEN reg.slot_id ELSE 0 END)
    and k1.report_date = reg.date_ 

LEFT JOIN kpi_v2 k2 
    on k2.uid = reg.uid 
    and k2.slot_id = (CASE 
                          WHEN k2.slot_id > 0 THEN reg.slot_id ELSE 0 END)  
    and k2.report_date = reg.date_ 

LEFT JOIN metrics a 
    on a.shipper_id = reg.uid 
    and a.inflow_date = reg.date_ 
    and a.slot_id = (CASE
                         WHEN a.slot_id > 0 then reg.slot_id ELSE 0 END)

WHERE 1 = 1 
and reg.date_ BETWEEN current_date - interval '90' day and current_date - interval '1' day

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24