select 
        a.created_date,
        a.hour_,
        count(distinct id) as gross_order,
        min_by(weather,created_timestamp) as weather,
        avg(c_temperature) as avg_c_temperature,
        avg(c_temperature_real_feel) as avg_c_temperature_real_feel

from
(select 
        raw.id,
        raw.created_date,
        raw.created_timestamp,
        is_foody_delivery,
        hour(raw.created_timestamp) as hour_,
        dt.weather,
        dt.c_temperature_real_feel,
        dt.c_temperature

from driver_ops_raw_order_tab raw


left join (select id,is_foody_delivery 
from shopeefood.shopeefood_mart_dwd_vn_order_completed_da where date(dt) = current_date - interval '1' day) oct 
        on raw.id = oct.id

left join driver_ops_temp dt 
        on raw.created_date = date(dt.start_time)
        and raw.created_timestamp between dt.start_time and dt.end_time
        and raw.city_id = dt.city_id
        and raw.district_id = dt.district_id
    
where is_foody_delivery = 1
and order_type = 0
and created_date between date'2022-01-01' and date'2023-12-31'
and raw.city_id = 217
) a 
where hour_ between 8 and 21
group by 1,2