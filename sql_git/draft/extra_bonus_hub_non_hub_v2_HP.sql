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
    and dot.ref_order_category = 0 -- Now Food only 
    and dot.pick_city_id not in (0,238,468,469,470,471,472)
    and date(from_unixtime(dot.submitted_time-3600)) between date '2022-05-20' and current_date - interval '1' day
)
,order_on_06_june as
(select 
    do.*
    ,case 
        -- when EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) >= 0600 AND EXTRACT(HOUR from raw.inflow_timestamp)*100+ EXTRACT(MINUTE from raw.inflow_timestamp) < 1030 then '06am_1030am'
        when EXTRACT(HOUR from do.inflow_timestamp)*100+ EXTRACT(MINUTE from do.inflow_timestamp) >= 1000 AND EXTRACT(HOUR from do.inflow_timestamp)*100+ EXTRACT(MINUTE from do.inflow_timestamp) < 1400 then '10am_14pm'
        when EXTRACT(HOUR from do.inflow_timestamp)*100+ EXTRACT(MINUTE from do.inflow_timestamp) >= 1600 AND EXTRACT(HOUR from do.inflow_timestamp)*100+ EXTRACT(MINUTE from do.inflow_timestamp) < 2000 then '16pm_20pm'       
        else null end as hour_range
    ,case 
    when pm.shipper_type_id = 12 then 'hub' 
    else 'non-hub' end as shipper_type
    ,pm.city_name
from driver_order do
left join shopeefood.foody_mart__profile_shipper_master pm 
    on do.shipper_id = pm.shipper_id and do.report_date = try_cast(pm.grass_date as date)
where do.inflow_date = date '2022-06-06'
)
,delivered_order_from_26may as
(
    select  
        shipper_id
        ,count(distinct order_uid) as delivered_orders
    from driver_order
    where report_date between date '2022-05-26' and date '2022-06-02'
    group by 1
)
,final as
(select 
    o1.shipper_id
    ,o1.shipper_type
    ,o1.city_name
    ,coalesce(o2.delivered_orders,0) as total_26may_02jun_delivered_orders
    ,count(distinct order_uid) as total_order_06_june
from order_on_06_june o1
left join delivered_order_from_26may o2
    on o1.shipper_id = o2.shipper_id
where hour_range in ('10am_14pm','16pm_20pm')
and o1.city_name = 'Hai Phong City'
group by 1,2,3,4
)
select 
    *
from final
where total_order_06_june >= 6
and total_26may_02jun_delivered_orders = 0
-- having count(distinct order_uid) >= 6