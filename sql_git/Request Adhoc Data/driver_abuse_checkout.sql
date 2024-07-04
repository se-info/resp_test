with checkin_log as 
(select 
        uid,
        from_unixtime(checkin_time - 3600) as checkin_time,
        from_unixtime(checkout_time - 3600) as checkout_time,
        from_unixtime(create_time - 3600) as create_time,
        from_unixtime(update_time - 3600) as update_time,
        status,
        date(from_unixtime(create_time - 3600)) as created_date

from shopeefood.foody_partner_db__shipper_checkin_checkout_log_tab__reg_daily_s0_live 
)
select
        created_date,
        diff_range,
        count(distinct order_code) as total_order,
        count(distinct shipper_id) as unique_driver,
        avg(diff_incharged_to_checkout) as avg_diff_incharged_to_checkout,
        avg(distance) as avg_distance 

from
(select 
        raw.order_code,
        raw.group_id,
        source,
        shipper_id,
        raw.created_timestamp,
        coalesce(raw.last_incharge_timestamp,raw.returned_timestamp) as final_timestamp,
        raw.delivered_timestamp,
        c.checkout_time,
        date_diff('second',raw.last_incharge_timestamp,c.checkout_time)*1.0000/60 as diff_incharged_to_checkout,
        raw.created_date,
        case 
        when (date_diff('second',raw.last_incharge_timestamp,c.checkout_time)*1.0000/60) <= 1 then '1. <= 1m'
        when (date_diff('second',raw.last_incharge_timestamp,c.checkout_time)*1.0000/60) <= 3 then '2. 1 - 3m'
        when (date_diff('second',raw.last_incharge_timestamp,c.checkout_time)*1.0000/60) <= 5 then '3. 3 - 5m'
        else '4 ++5m' end as diff_range,
        raw.distance



from driver_ops_raw_order_tab raw 
left join checkin_log c
    on c.uid = raw.shipper_id 
    and c.created_date = raw.created_date
    and c.update_time between raw.last_incharge_timestamp and raw.picked_timestamp

where raw.order_type != 0
and c.update_time is not null 
and raw.created_date between current_date - interval '7' day and current_date - interval '1' day
and raw.order_status in ('Delivered','Returned')
and group_id = 0
)
group by 1,2
