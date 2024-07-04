with fa as                
(SELECT   
    order_id 
    , 0 as order_type
    ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
    ,max(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_auto_assign_timestamp
    ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
    ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
    from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
    where 1=1
    and grass_schema = 'foody_order_db'
    group by 1,2
)
,driver_order as 
(select 
        dot.uid as shipper_id
        ,dot.ref_order_id as order_id
        ,dot.ref_order_code as order_code
        ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
        ,dot.ref_order_category
        ,case when dot.ref_order_category = 0 then 'order_delivery'
            when dot.ref_order_category = 3 then 'now_moto'
            when dot.ref_order_category = 4 then 'now_ship'
            when dot.ref_order_category = 5 then 'now_ship'
            when dot.ref_order_category = 6 then 'now_ship_shopee'
            when dot.ref_order_category = 7 then 'now_ship_sameday'
            else null end source
        ,dot.ref_order_status
        ,dot.order_status
        ,case when dot.order_status = 1 then 'Pending'
            when dot.order_status in (100,101,102) then 'Assigning'
            when dot.order_status in (200,201,202,203,204) then 'Processing'
            when dot.order_status in (300,301) then 'Error'
            when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
            else null end as order_status_group
        ,dot.is_asap
        -- ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
        --     when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
        --     else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
        ,case 
            when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60)) 
            else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
        -- ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
        ,from_unixtime(dot.submitted_time- 60*60) created_timestamp
        ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
    --   ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
        ,case when dot.pick_city_id = 217 then 'HCM'
            when dot.pick_city_id = 218 then 'HN'
            when dot.pick_city_id = 219 then 'DN'
            ELSE 'OTH' end as city_group
    ,coalesce(fa.last_auto_assign_timestamp, from_unixtime(dot.submitted_time - 3600)) as inflow_timestamp
    ,date(coalesce(fa.last_auto_assign_timestamp, from_unixtime(dot.submitted_time - 3600))) as inflow_date
    ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
    on dot.ref_order_id = oct.id and dot.ref_order_category = 0 and oct.submit_time > 1609439493
left join fa
    on fa.order_id = oct.id
left join 
    (SELECT order_id
        ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
        ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)
        -- ,order_data
    from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

    )dotet on dot.id = dotet.order_id

where 1=1
    and dot.order_status = 400 -- delivered order
    -- and dot.ref_order_category = 0 -- Now Food only 
    -- and dot.pick_city_id not in (0,238,468,469,470,471,472)

    )
, kpi_qualified AS
(SELECT
    hub.uid AS shipper_id
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

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_id = 217   
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/180 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' then 1 
--and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.000/3600 >= 1 then 1 

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_id = 218
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/180 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' 
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 then 1 

else 0 end as is_qualified_kpi
FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_income_report_mapping_tab__reg_daily_s0_live hubm on hubm.report_id = hub.id 

WHERE DATE(FROM_UNIXTIME(hub.report_date - 3600)) BETWEEN current_date - interval '45' day and current_date - interval '1' day
)

,final as
(select 
    do.*
    ,case 
        -- when EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) >= 0600 AND EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) < 1030 then '06am_1030am'
        when EXTRACT(HOUR from do.last_delivered_timestamp)*100+ EXTRACT(MINUTE from do.last_delivered_timestamp) >= 1100 AND EXTRACT(HOUR from do.last_delivered_timestamp)*100+ EXTRACT(MINUTE from do.last_delivered_timestamp) <= 1300 then '11_13'
            -- EXTRACT(HOUR from do.inflow_timestamp)*100+ EXTRACT(MINUTE from do.inflow_timestamp) >= 1800 AND EXTRACT(HOUR from do.inflow_timestamp)*100+ EXTRACT(MINUTE from do.inflow_timestamp) < 2100) then '10am_13pm_&_18pm_21pm'
        when EXTRACT(HOUR from do.last_delivered_timestamp)*100+ EXTRACT(MINUTE from do.last_delivered_timestamp) >= 1700 AND EXTRACT(HOUR from do.last_delivered_timestamp)*100+ EXTRACT(MINUTE from do.last_delivered_timestamp) <= 1900 then '17_     19'       
        else 'Other range' end as hour_range
    -- ,case 
    -- when pm.shipper_type_id = 12 AND slot.registration_status != 'OFF'  then 'hub' 
    -- else 'non-hub' end as shipper_type
    -- ,pm.city_name
from driver_order do



where 1 = 1 
-- and date_format(do.inflow_date,'%a') = 'Sun'
and do.report_date = date'2022-08-08'
)

select 
        do.inflow_date 
       ,do.shipper_id
       ,pm.shipper_name
       ,pm.city_name 
    --    ,do.hour_range 
       ,case 
        when pm.shipper_type_id = 12 then 'hub' 
        else 'non-hub' end as shipper_type
    --    ,case 
    --     when pm.shipper_type_id = 12 AND slot.registration_status != 'OFF'  then kpi.is_qualified_kpi
    --     else dr.completed_rate*1.00/100 end as kpi_n_sla
    --    ,case 
    --     when pm.shipper_type_id = 12 AND slot.registration_status != 'OFF' and kpi.is_qualified_kpi > 0 then 1
    --     when pm.shipper_type_id = 12 AND slot.registration_status = 'OFF' and  dr.completed_rate*1.00/100 >= 85 then 1
    --     when pm.shipper_type_id != 12 AND  dr.completed_rate*1.00/100 >= 85 then 1 
    --    else 0 end as is_qualified               
    --    ,count(distinct case when hour_range = '11_13' then do.order_id else null end) as total_order_11_13
    --    ,count(distinct case when hour_range = '17_19' then do.order_id else null end) as total_order_17_19
       ,count(distinct do.order_id) as total_order 


from final do 

-- left join kpi_qualified kpi on kpi.shipper_id = do.shipper_id and do.report_date = kpi.report_date

-- left join 
-- (SELECT 
--  date(from_unixtime(date_ts - 3600)) as date_
-- ,uid
-- ,case when registration_status = 1 then 'Registered'
--       when registration_status = 2 then 'OFF'
--       when registration_status = 3 then 'Worked'
--       end as registration_status
-- ,(end_time - start_time)/3600 as shift_hour
-- ,slot_id
-- FROM shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live
--     ) slot on slot.uid = do.shipper_id and do.inflow_date = slot.date_  

-- left join shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live dr on dr.uid = do.shipper_id and date(from_unixtime(dr.report_date - 3600)) = do.inflow_date    

left join shopeefood.foody_mart__profile_shipper_master pm 
    on do.shipper_id = pm.shipper_id and do.report_date = try_cast(pm.grass_date as date)    

where 1 = 1 
-- and do.shipper_id in 


group by 1,2,3,4,5






