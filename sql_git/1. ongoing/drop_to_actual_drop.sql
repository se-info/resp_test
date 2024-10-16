with log_order as
(select 
        order_id,
        location,
        order_type,
        split(location,',') as location_list

from shopeefood.foody_partner_db__order_shipper_status_log_tab__reg_daily_s0_live l
where status = 7
and location != ''
)
select 
        shipper_id,
        count(1)
from
(select 
        r.id,
        r.shipper_id,
        date(r.delivered_timestamp) as report_date,
        GREAT_CIRCLE_DISTANCE(cast(l.location_list[1] as double),cast(l.location_list[2] as double),drop_latitude,drop_longitude) as distance_actual_drop_to_user



from driver_ops_raw_order_tab r 

left join log_order l on l.order_id = r.id and l.order_type = r.order_type

where r.driver_policy = 2
and date(r.delivered_timestamp) = date'2024-10-08'
and GREAT_CIRCLE_DISTANCE(cast(l.location_list[1] as double),cast(l.location_list[2] as double),drop_latitude,drop_longitude) >= 2 
)
group by 1 