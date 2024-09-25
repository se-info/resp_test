with raw as 
(select 
        dot.group_id,
        case 
        when ogi.group_distance <= 2 then '1. 0 - 2km'
        when ogi.group_distance <= 4 then '2. 2 - 4km'
        when ogi.group_distance <= 6 then '3. 4 - 6km'
        when ogi.group_distance <= 8 then '4. 6 - 8km'
        when ogi.group_distance <= 10 then '5. 8 - 10km'
        when ogi.group_distance > 10 then '6. ++10km'
        end as distance_range,
        ogi.group_fee as delivery_cost,
        ogi.group_distance,
        case 
        when ogi.ref_order_category = 0 then 'delivery'
        when ogi.ref_order_category = 6 then 'ecome'
        else 'c2c' end as source,
        r.late_night_service_fee,
        r.bad_weather_fee,
        r.holiday_service_fee,
        r.city_group,
        count(distinct dot.ref_order_id) as cnt_order_in_group,
        max_by(dot.pick_city_id,submitted_time) as city_id,
        max(date(from_unixtime(dot.real_drop_time - 3600))) as report_date


from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da dot 

left join 
(select 
        group_id,
        order_type,
        min_by(r.city_group,r.last_incharge_timestamp) as city_group,
        SUM(r.late_night_service_fee) as late_night_service_fee,
        SUM(r.holiday_service_fee) as holiday_service_fee,
        SUM(r.bad_weather_fee) as bad_weather_fee
        
-- id,late_night_service_fee,bad_weather_fee,holiday_service_fee,order_type,city_group
-- select *
from driver_ops_raw_order_tab r 
where 1 = 1 
and order_status = 'Delivered'
group by 1,2 
) r on r.group_id = dot.group_id and r.order_type = dot.ref_order_category

left join
(select 
        id,
        ref_order_category,
        ship_fee*1.0000/100 as group_fee,
        distance*1.00/100000 as group_distance
        
from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da
where date(dt) = current_date - interval '1' day
) ogi on ogi.id = dot.group_id and ogi.ref_order_category = dot.ref_order_category

where date(dot.dt) = current_date - interval '1' day
and date(from_unixtime(dot.real_drop_time - 3600)) between date'2024-07-01' and date'2024-09-22'
and dot.order_status = 400
and dot.group_id > 0
group by 1,2,3,4,5,6,7,8,9
)
select
        date_trunc('week',report_date) as start_date_of_week,
        distance_range,
        city_group,
        source,
        count(distinct group_id) as cnt_group,
        sum(cnt_order_in_group) as cnt_order,
        sum(group_distance)*1.0000 as avg_group_distance,
        sum(delivery_cost + holiday_service_fee + late_night_service_fee + bad_weather_fee) as total_shipping_fee

from raw  
where raw.city_id in (217,218)
group by 1,2,3,4 
