with dt_array_tab as
(
    select 1 mapping, sequence(cast('2024-02-01 00:00:00' as timestamp ), cast('2024-02-26 23:59:59' as timestamp ), interval '3600' second  ) dt_array
) 
,list_time_range as
(select 
    t1.mapping
    ,t2.dt_array as start_time
    ,t2.dt_array + interval '3599.99' second as end_time
from dt_array_tab t1
cross join unnest (dt_array) as t2(dt_array)
) 
,holiday_tab as 
(select 
        custom_date as adjust_date,
        city_id,
        'holiday_fee' as metrics,
        start_time + interval '1' day * (date_diff('day',cast(start_time as date),custom_date)) as start_time_timestamp,
        end_time + interval '1' day * (date_diff('day',cast(end_time as date),custom_date)) as end_time_timestamp,
        -- date_format(start_time + interval '30' minute,'%H:%i:%S') as start_time,
        -- date_format(end_time + interval '30' minute,'%H:%i:%S') as end_time,
        value as fee_amount

from shopeefood.foody_delivery_admin_db__setting_time_range_tab__vn_daily_s0_live
-- where 1 = 1 
where custom_date between date'2024-02-01' and date'2024-02-26'
and city_id in (217,218)
and name = 'order.holiday_service_fee.amount'
and create_uid != 15594639 -- # excluded now_admin uid
)
,fee as 
(select 
        raw.id as ref_id,
        case 
        when order_type = 0 then 'food' else 'e2c' end as source,
        raw.delivery_id,
        raw.city_id,
        doet.surge_rate,
        doet.min_fee,
        created_timestamp,
        raw.bad_weather_fee,
        raw.late_night_service_fee,
        lt.start_time,
        lt.end_time,
        raw.created_date


from driver_ops_raw_order_tab raw 

left join list_time_range lt 
    on raw.created_timestamp between lt.start_time and lt.end_time

left join 
(select 
        order_id,
        cast(json_extract(order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate,
        cast(json_extract(order_data,'$.shipping_fee_config.min_fee') as double) as min_fee

from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da 
where date(dt) = current_date - interval '1' day
) doet 
    on doet.order_id = raw.delivery_id

where raw.created_date >= date'2024-02-01'
and raw.order_status = 'Delivered'
and raw.shipper_id > 0
and surge_rate is not null
-- and raw.order_type = 6
)
select
        created_date,
        source,
        city_id,
        start_time,
        end_time,
        avg(min_fee) as driver_min_fee_avg,
        array_agg(distinct min_fee) as driver_min_fee_ext_info,
        avg(surge_rate) as driver_surge_rate_avg,
        array_agg(distinct surge_rate) as driver_surge_rate_ext_info,
        avg(bad_weather_fee) as bad_weather_fee_avg,
        array_agg(distinct bad_weather_fee) as bad_weather_fee_ext_info,
        max(late_night_service_fee) as late_night_service_fee

from fee 

where city_id in (217,218)
group by 1,2,3,4,5