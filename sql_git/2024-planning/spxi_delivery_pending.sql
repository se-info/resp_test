with sp_log as 
(select 
        booking_id,
        booking_type,
        cast(json_extract(old_value,'$.code') as varchar) as order_code,
        split(message,' ') as message,
        from_unixtime(update_time - 3600) as updated
        -- FILTER(split(message,' '),x->x='DELIVERY PENDING')

from shopeefood.foody_express_db__shopee_booking_change_log_tab__reg_daily_s0_live
where 1 = 1 
and cardinality(FILTER(split(message,' '),x->regexp_like(x,'DELIVERY_PENDING'))) > 0
)
,raw as 
(select 
        r.id,
        r.order_code,
        r.order_status,
        -- s.message,
        case 
        when s.message is not null then 1 else 0 end as is_delivery_pending,
        s.updated as delivery_pending_time,
        case 
        when r.order_status = 'Delivered' then delivered_timestamp
        when r.order_status = 'Returned' then returned_timestamp
        end as final_timestamp,
        r.distance,
        created_date

from driver_ops_raw_order_tab r 

left join sp_log s 
        on s.order_code = r.order_code 
        -- and s.booking_type = r.order_type

where 1 = 1 
and order_type = 6
and order_status in ('Delivered','Returned')
)
select *
-- select
--         report_date,
--         order_status as final_status_order,
--         is_delivery_pending as is_have_delivery_pending_status,
--         count(distinct order_code) as total_order,
--         avg(distance) as avg_distance,
--         approx_percentile(distance,0.7) as pct_70th_distance,
--         approx_percentile(distance,0.8) as pct_80th_distance,
--         approx_percentile(distance,0.9) as pct_90th_distance,
--         approx_percentile(distance,0.95) as pct_95th_distance,
--         sum(case when duration_pending_to_final > 0  then duration_pending_to_final else null end)/
--         cast(count(distinct case when duration_pending_to_final > 0 then order_code else null end) as double) as avg_duration_pending_to_final,
--         approx_percentile(duration_pending_to_final,0.95) as pct_95th_duration_pending_to_final


from
(select 
        date(final_timestamp) as report_date,
        order_status,
        is_delivery_pending,
        case 
        when is_delivery_pending is not null then date_diff('second',delivery_pending_time,final_timestamp)*1.0000/60
        else 0 end as duration_pending_to_final,
        order_code,
        distance,
        created_date

from raw 
)
where created_date between date'2024-01-01' and date'2024-02-05'
-- group by 1,2,3



