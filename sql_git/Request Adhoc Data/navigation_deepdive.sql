with status_log as 
(select 
        t1.order_id,
        t1.order_type,
        t1.location
        -- split(t1.location,',') as arr_location,
        -- ST_Distance(to_spherical_geography(ST_Point(cast(split(t1.location,',')[2] as double),cast(split(t1.location,',')[1] as double)))
        --         , to_spherical_geography(ST_Point(drop_longitude,drop_latitude))) as actual_delivered_to_real_drop

from shopeefood.foody_partner_db__order_shipper_status_log_tab__reg_daily_s0_live t1 

left join driver_ops_raw_order_tab t2 
        on t2.id = t1.order_id and t1.order_type = t2.order_type

where t1.status = (case when t1.order_type = 0 then 7 else 11 end)
and length(t1.location) < 1
)
,metrics as 
(select 
        t1.grass_date,
        t1.shipper_id,
        t1.ref_order_id,
        t1.is_one_order_mode,
        t1.to_pin_distance,
        s.actual_delivered_to_real_drop,
        case 
        when t1.shipper_id = t2.shipper_id then 1 else 0 end is_delivered,
        case 
        when to_pin_distance <= pin_range then 1 else 0 end as is_navigation_success,
        t2.city_id,
        t2.city_name as order_city,
        t2.delivered_timestamp
        -- count(distinct case when t1.is_one_order_mode = true then t1.ref_order_id else null end) as sinlge_navigate,
        -- count(distinct case when t1.is_one_order_mode = false then t1.ref_order_id else null end) as multi_navigate,
        -- avg(t1.to_pin_distance) as avg_drop_to_pin

from driver_ops_navigation_order_raw_tab t1

left join driver_ops_raw_order_tab t2 
        on t1.ref_order_id = t2.id 

-- gap distance to real drop point > 300m => pre delivered
left join status_log s 
        on t1.ref_order_id = s.order_id


-- where t1.shipper_id = 23092653
where 1 = 1 
and t1.grass_date between current_date - interval '14' day and current_date - interval '1' day
-- and t1.driving_distance > 0
-- and t1.flying_distance > 0
)
,f as 
(select
-- shipper_id = 20226193 and grass_date = date'2023-12-18'
        m.grass_date,
        m.shipper_id,
        case 
        when max_by(order_city,delivered_timestamp) in ('HCM City','Ha Noi City','Da Nang City') then max_by(order_city,delivered_timestamp)
        else 'Other' end as city_group,
        -- order_city,        
        count(case when is_one_order_mode = true then ref_order_id else null end) as one_mode_navigate_request_turn,
        count(case when is_one_order_mode = false then ref_order_id else null end) as multi_mode_navigate_request_turn,
        count(distinct ref_order_id) as total_navigation_request_turn,
        count(case when is_navigation_success = 1 then ref_order_id else null end) as navigation_success,
        count(case when is_navigation_success = 1 then ref_order_id else null end)/cast(count(distinct ref_order_id) as double) as navigation_success_rate,
        avg(actual_delivered_to_real_drop) as avg_actual_delivered_to_real_drop_point
from metrics m

group by 1,2
)
select 
        grass_date,
        city_group,
        navigation_request_range,
        count(distinct shipper_id) as total_driver,
        avg(total_navigation_request_turn) as avg_total_navigation_request_turn,
        approx_percentile(total_navigation_request_turn,0.8) as pct80_total_navigation_request_turn, 
        approx_percentile(total_navigation_request_turn,0.9) as pct90_total_navigation_request_turn,
        approx_percentile(total_navigation_request_turn,0.95) as pct95_total_navigation_request_turn
        
from
(select 
        f.grass_date,
        f.shipper_id,
        f.city_group,
        f.total_navigation_request_turn,
        case 
        when f.total_navigation_request_turn < 3 then '1. 0 - 3 turn'
        when f.total_navigation_request_turn < 5 then '2. 3 - 5 turn'
        when f.total_navigation_request_turn < 8 then '3. 5 - 8 turn'
        when f.total_navigation_request_turn < 10 then '4. 8 - 10 turn'
        when f.total_navigation_request_turn < 20 then '5. 10 - 20 turn'
        when f.total_navigation_request_turn < 30 then '6. 20 - 30 turn'
        when f.total_navigation_request_turn <= 40 then '8. 30 - 40 turn'
        when f.total_navigation_request_turn > 40 then '9. ++ 40 turn' end as navigation_request_range 

from f 
)
group by 1,2,3
