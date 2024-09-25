WITH raw as 
(select 
        group_id,
        ogi.re as group_re,
        min_by(city_name,last_incharge_timestamp) as city_name,
        date_trunc('month',max(date(delivered_timestamp))) as report_month,
        COUNT(DISTINCT order_code) as cnt_order_in_group,
        CARDINALITY(ARRAY_AGG(pick_latitude)) AS count_pick_lat,
        CARDINALITY(ARRAY_AGG(DISTINCT pick_latitude)) AS count_pick_lat_unique,
        CARDINALITY(ARRAY_AGG(pick_longitude)) AS count_pick_long,
        CARDINALITY(ARRAY_AGG(DISTINCT pick_longitude)) AS count_pick_long_unique,
        
        CARDINALITY(ARRAY_AGG(drop_latitude)) AS count_drop_lat,
        CARDINALITY(ARRAY_AGG(DISTINCT drop_latitude)) AS count_drop_lat_unique,
        CARDINALITY(ARRAY_AGG(drop_longitude)) AS count_drop_long,
        CARDINALITY(ARRAY_AGG(DISTINCT drop_longitude)) AS count_drop_long_unique


from driver_ops_raw_order_tab r 

left join 
(select 
        id,
        cast(json_extract_scalar(ogi.extra_data,'$.re') as double) as re

from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da ogi
where date(dt) = current_date - interval '1' day
) ogi on ogi.id = r.group_id

where date(delivered_timestamp) >= date'2024-07-01' 
and source = 'order_food'
and order_status = 'Delivered'
and group_id > 0
group by 1,2
)
select 
        report_month,
        cnt_order_in_group,
        pick_route,
        city_name,
        -- drop_route,
        count(distinct group_id) as "tổng số đơn group",
        sum(cnt_order_in_group) as "tổng số đơn con trong group",
        sum(group_re)*1.0000/count(distinct group_id) as "re trung bình của group"

from
(select 
        *,
        CASE 
            WHEN count_pick_lat_unique = count_pick_long_unique AND count_pick_lat_unique = 1 THEN 'same_pick'
            WHEN count_pick_lat_unique = count_pick_long_unique AND count_pick_lat_unique > 1 THEN 'diff_pick'
            WHEN count_pick_lat_unique != count_pick_long_unique THEN 'diff_pick' END AS pick_route,
        CASE 
            WHEN count_drop_lat_unique = count_drop_long_unique AND count_drop_lat_unique = 1 THEN 'same_drop'
            WHEN count_drop_lat_unique = count_drop_long_unique AND count_drop_lat_unique > 1 THEN 'diff_drop' 
            WHEN count_drop_lat_unique != count_drop_long_unique THEN 'diff_drop' END AS drop_route      

from raw
)
where city_name in ('HCM City','Ha Noi City')
group by 1,2,3,4 