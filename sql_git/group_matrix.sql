with 
group_info_tab as (
    select 
        id as group_id
        , group_code
        , ref_order_category
        , distance * 1.00 / 100 as group_distance
        , ship_fee * 1.00 / 100 as group_fee
        , uid as shipper_uid
        , group_status
        , create_time
        , cast(json_extract(extra_data, '$.re') as double) AS re 
        , cast(json_extract(extra_data, '$.pick_city_id') as int) AS city_id
        , json_array_length(json_extract(extra_data, '$.distance_matrix.mapping')) / 2 as group_order_cnt
        , ship_fee * 1.00 / 100 / (json_array_length(json_extract(extra_data, '$.distance_matrix.mapping'))/2) as group_fee_per_order
        , cast(json_extract(extra_data, '$.distance_matrix.data') as ARRAY(ARRAY(integer))) as distance_matrix
        , cast(json_extract(extra_data, '$.distance_matrix.mapping') as ARRAY(integer)) as mapping
        , extra_data
    from shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live
    where date_format(from_unixtime(create_time, 7, 0), '%Y-%m-%d') >= '${start_date}'
    and date_format(from_unixtime(create_time, 7, 0), '%Y-%m-%d') <= '${end_date}'
)

, group_map_tab as (
        select 
            *
            , google_distance - ds_distance as distance_gap
        from ( 
            select 
                date_format(from_unixtime(t1.create_time, 7, 0), '%Y-%m-%d') as dtt
                , t1.group_id
                , t1.order_id as delivery_id
                , t2.ref_order_id as order_id
                , t2.uid as shipper_uid
                , t2.delivery_distance as google_distance
                , t2.pick_latitude
                , t2.pick_longitude
                , t1.ref_order_category
                , t1.mapping_status
                , t1.create_time 
                , CAST(json_extract(t1.extra_data,'$.ship_fee_info.driver_ship_fee') AS DOUBLE) AS single_fee
                , t3.group_distance
                , t3.group_fee
                , t3.re 
                , t3.city_id
                , t3.group_order_cnt
                , t3.group_fee_per_order
                , t3.distance_matrix
                , t3.mapping
                , array_position(t3.mapping, -1 * t1.order_id) as mapping_pick_index
                , array_position(t3.mapping, t1.order_id) as mapping_drop_index
                , t3.distance_matrix[array_position(t3.mapping, -1 * t1.order_id)][array_position(t3.mapping, t1.order_id)] as ds_distance
            from shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live t1
            left join shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live t2 on t1.order_id = t2.id
            left join group_info_tab t3 on t1.group_id = t3.group_id
            where mapping_status not in (20, 21, 22, 24, 25)
            and date_format(from_unixtime(t1.create_time, 7, 0), '%Y-%m-%d') >= '${start_date}'
            and date_format(from_unixtime(t1.create_time, 7, 0), '%Y-%m-%d') <= '${end_date}'
        )
        where mapping_pick_index > 0 
        and mapping_drop_index > 0
        
)

select * from group_map_tab