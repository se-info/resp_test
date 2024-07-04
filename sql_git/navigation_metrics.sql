with 
ab_hit_log_tab as (
    select 
        t1.*
        , t2.uid as shipper_id
    from (
        select 
            date_format(grass_date, '%Y-%m-%d') as grass_date
            , user_id as shopee_uid
            , exp_group_id
            , 'show' as group_name
        from tracking.shopeefooddriver_tracking_ods_ubt_merge_di__reg_s1_live
        where 1=1
        and grass_date >= date'${start_date}'
        and grass_date <= date'${end_date}'
        and exp_group_id[1] = 91933
    ) t1 
    left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live t2 on t1.shopee_uid = t2.shopee_uid
)

, basic_tab as (
    select 
        date_format(t1.dt, '%Y-%m-%d') as grass_date
        , t1.order_id
        , 0 as order_category
        , t1.city_id
        , t2.shipper_uid
        , t2.final_delivered_time
        , t2.ata
        , t1.is_net
        , case when t3.status = 7 then 1 else 0 end as is_delievered
        , case when t3.status = 9 then 1 else 0 end as is_quit
    from (
        select * from shopeefood_assignment.algo_g2n_data_funnel_reg_dwd__order_waybill_di
        where grass_region = 'VN'
        and date_format(dt, '%Y-%m-%d') >= '${start_date}'
        and date_format(dt, '%Y-%m-%d') <= '${end_date}'
    ) t1 
    left join (
        select 
            id as order_id
            , city_id
            , shipper_uid
            , final_delivered_time
            , cast(final_delivered_time - submit_time as double) / 60.0 as ata
        from shopeefood.shopeefood_mart_dwd_vn_order_completed_da
        where date(dt) = current_date - interval '1' day
        and date_format(from_unixtime(submit_time, 7, 0), '%Y-%m-%d') >= '${start_date}'
        and date_format(from_unixtime(submit_time, 7, 0), '%Y-%m-%d') <= '${end_date}'
    ) t2 on t1.order_id = t2.order_id
    left join (
        select 
            order_id
            , 0 as order_category 
            , shipper_uid
            , status    -- 7=delivered, 9=quit
        from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live 
        where status in (7, 9)
        and date_format(from_unixtime(create_time, 7, 0), '%Y-%m-%d') >= '${start_date}'
        and date_format(from_unixtime(create_time, 7, 0), '%Y-%m-%d') <= '${end_date}'
    ) t3 on t1.order_id = t3.order_id
    
    union all 

    select 
        date_format(from_unixtime(t1.create_time, 7, 0), '%Y-%m-%d') as grass_date
        , t1.id as order_id
        , 4 as order_category
        , t1.city_id
        , t1.shipper_id as shipper_uid
        , t1.status_update_time as final_delivered_time
        , cast(t1.status_update_time - t1.create_time as double) / 60.0 as ata
        , case when t1.status = 11 or t1.status = 14 then 1 else 0 end as is_net    -- 11=COMPLETED, 14=RETURN_SUCCESS
        , case when t1.status = 11 or t1.status = 14 then 1 else 0 end as is_delievered
        , case when t1.status = 9 then 1 else 0 end as is_quit  -- 9=SHIPPER_CANCELED
    from shopeefood.foody_express_db__booking_tab__reg_daily_s0_live t1
    where date_format(from_unixtime(t1.create_time, 7, 0), '%Y-%m-%d') >= '${start_date}'
    and date_format(from_unixtime(t1.create_time, 7, 0), '%Y-%m-%d') <= '${end_date}'

    union all 

    select 
        date_format(from_unixtime(t1.create_time, 7, 0), '%Y-%m-%d') as grass_date
        , t1.id as order_id
        , 6 as order_category
        , t1.city_id
        , t1.shipper_id as shipper_uid
        , t1.status_update_time as final_delivered_time
        , cast(t1.status_update_time - t1.create_time as double) / 60.0 as ata
        , case when t1.status = 11 or t1.status = 14 then 1 else 0 end as is_net    -- 11=COMPLETED, 14=RETURN_SUCCESS
        , case when t1.status = 11 or t1.status = 14 then 1 else 0 end as is_delievered
        , case when t1.status = 9 then 1 else 0 end as is_quit  -- 9=SHIPPER_CANCELED
    from shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live t1
    where date_format(from_unixtime(t1.create_time, 7, 0), '%Y-%m-%d') >= '${start_date}'
    and date_format(from_unixtime(t1.create_time, 7, 0), '%Y-%m-%d') <= '${end_date}'
)

, assign_tab as (
    select date_format(from_unixtime(create_time, 7, 0), '%Y-%m-%d') as dtt
        , if(t.status=3, 1, 0) as is_accept
        , if(t.status=2, 1, 0) as is_denied
        , if(t.status=9, 1, 0) as is_ignore
        , t.*
    from (
        SELECT id,order_id,order_type,city_id,district_id,shipper_uid,assign_type,status,location,experiment_group,extra_data,create_time,update_time,expiry_time
        FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        UNION ALL
        SELECT id,order_id,order_type,city_id,district_id,shipper_uid,assign_type,status,location,experiment_group,extra_data,create_time,update_time,expiry_time
        FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
    ) as t
    where date_format(from_unixtime(create_time, 7, 0), '%Y-%m-%d') >= '${start_date}'
    and date_format(from_unixtime(create_time, 7, 0), '%Y-%m-%d') <= '${end_date}'
)

------------------------------------------------------request------------------------------------------------------

, navigation_request_tab as (
    SELECT 
        id
        , city_id
        , processing_time
        , grass_date
        , create_time
        , date_format(FROM_UNIXTIME(create_time, 7, 0), '%Y-%m-%d %H:%i:%s') AS date_str
        
        , json_extract_scalar(processing_info, '$.ds_response["batch_id"]') AS request_id
        , json_extract(processing_info, '$.ds_response["results"]') AS ds_response_results

        , json_extract(processing_info, '$.ds_request') AS ds_request
        , json_extract(processing_info, '$.ds_request["info"]') AS ds_request_info
        , json_extract(processing_info, '$.ds_request["order_shippers"]') AS ds_request_orders
        , json_extract(processing_info, '$.ds_request["shippers"]') AS ds_request_shippers
        , json_extract(processing_info, '$.ds_request["vehicles"]') AS ds_request_vehicles
        , cast(json_extract(processing_info, '$.ds_request["config"]["navigation_config"]["pin_range"]') as bigint) AS pin_range
    from shopeefood.foody_partner_archive_db__order_assign_shipper_batch_processing_log_tab_di
    WHERE not json_extract(processing_info, '$.ds_request["info"]') is null
    -- and processing_info like '%pin_lon%'
    and grass_date >= '${start_date}'
    and grass_date <= '${end_date}'
    order by create_time
)

, navigation_stack_request_tab as (
    SELECT 
        id
        , city_id
        , processing_time
        , grass_date
        , create_time
        , date_format(FROM_UNIXTIME(create_time, 7, 0), '%Y-%m-%d %H:%i:%s') AS date_str

        , json_extract(processing_info, '$.ds_stack_response') AS ds_stack_response
        , json_extract(processing_info, '$.ds_stack_response["results"]') AS ds_response_results
        , json_extract(processing_info, '$.ds_stack_response["unassigned_orders"]') AS ds_unassigned_orders
        , json_extract_scalar(processing_info, '$.ds_stack_request["info"]["stack_id"]') AS request_id

        , json_extract(processing_info, '$.ds_stack_request') AS ds_stack_request
        , json_extract(processing_info, '$.ds_stack_request["info"]') AS ds_request_info
        , json_extract(processing_info, '$.ds_stack_request["shippers"]') AS ds_request_shippers
        , json_extract(processing_info, '$.ds_stack_request["stacking_orders"]') AS ds_request_orders
        , cast(json_extract(processing_info, '$.order_category') as bigint) AS order_category
        , cast(json_extract(processing_info, '$.ds_stack_request["info"]["pin_range"]') as bigint) AS pin_range
    from shopeefood.foody_partner_archive_db__order_assign_shipper_batch_processing_log_tab_di
    WHERE not json_extract(processing_info, '$.ds_stack_request["info"]') is null
    -- and processing_info like '%pin_lon%'
    and grass_date >= '${start_date}'
    and grass_date <= '${end_date}'
    order by create_time
)

, request_shippers_tab AS (
    SELECT
        id
        , city_id
        , date_str
        , grass_date
        , create_time
        , request_id
        , pin_range
        , 'single' as assign_type
        , if(json_extract(value, '$.navigation_info["pin_lon"]') is null, 0, 1) AS is_navigation_shipper
        , cast(json_extract(value, '$.navigation_info["navigation_timestamp"]') as bigint) AS navigation_timestamp
        , cast(json_extract(value, '$.navigation_info["pin_lon"]') as double) AS pin_lon
        , cast(json_extract(value, '$.navigation_info["pin_lat"]') as double) AS pin_lat
        , cast(shipper_id as bigint) as shipper_id
        , cast(json_extract(value, '$.lon') as double) AS lon
        , cast(json_extract(value, '$.lat') as double) AS lat
    FROM navigation_request_tab
    CROSS JOIN UNNEST(cast(ds_request_shippers as map<varchar,json>)) AS x(shipper_id, value)

    union all 

    SELECT
        id
        , city_id
        , date_str
        , grass_date
        , create_time
        , request_id
        , pin_range
        , 'stack' as assign_type
        , if(json_extract(value, '$.navigation_info["pin_lon"]') is null, 0, 1) AS is_navigation_shipper
        , cast(json_extract(value, '$.navigation_info["navigation_timestamp"]') as bigint) AS navigation_timestamp
        , cast(json_extract(value, '$.navigation_info["pin_lon"]') as double) AS pin_lon
        , cast(json_extract(value, '$.navigation_info["pin_lat"]') as double) AS pin_lat
        , cast(shipper_id as bigint) as shipper_id
        , cast(json_extract(value, '$.lon') as double) AS lon
        , cast(json_extract(value, '$.lat') as double) AS lat
    FROM navigation_stack_request_tab
    CROSS JOIN UNNEST(cast(ds_request_shippers as map<varchar,json>)) AS x(shipper_id, value)
)

, request_orders_tab AS (
    SELECT
        id
        , city_id
        , date_str
        , grass_date
        , create_time
        , request_id
        , 'single' as assign_type
        , order_id
        , cast(SPLIT(order_id, '_')[2] as bigint) as ref_order_id
        , cast(json_extract(value, '$.p_lat') as double) AS p_lat
        , cast(json_extract(value, '$.p_lon') as double) AS p_lon
        , cast(json_extract(value, '$.d_lat') as double) AS d_lat
        , cast(json_extract(value, '$.d_lon') as double) AS d_lon
    FROM navigation_request_tab
    CROSS JOIN UNNEST(cast(ds_request_orders as map<varchar,json>)) AS x(order_id, value)

    union all 

    select 
        t1.id
        , t1.city_id
        , t1.date_str
        , t1.grass_date
        , t1.create_time
        , t1.request_id
        , t1.assign_type
        , t1.delivery_id as order_id
        , t2.ref_order_id
        , t1.p_lat
        , t1.p_lon
        , t1.d_lat
        , t1.d_lon
    from (
        SELECT
            id
            , city_id
            , date_str
            , grass_date
            , create_time
            , request_id
            , 'stack' as assign_type
            , delivery_id
            , cast(json_extract(value, '$.p_lat') as double) AS p_lat
            , cast(json_extract(value, '$.p_lon') as double) AS p_lon
            , cast(json_extract(value, '$.d_lat') as double) AS d_lat
            , cast(json_extract(value, '$.d_lon') as double) AS d_lon
        FROM navigation_stack_request_tab
        CROSS JOIN UNNEST(cast(ds_request_orders as map<varchar,json>)) AS x(delivery_id, value)
    ) t1
    left join shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live t2 on cast(t1.delivery_id as bigint) = t2.id
)

, navigation_shipper_request_cnt_tab as (
    select 
        grass_date
        , city_id
        , shipper_id
        , sum(is_navigation_shipper) as navigation_shipper_request_cnt
    from (select * from request_shippers_tab where is_navigation_shipper = 1)
    group by 1, 2, 3
)

, navigation_shipper_request_times_cnt_tab as (
    select 
        grass_date
        , city_id
        , shipper_id
        , navigation_timestamp
        , sum(is_navigation_shipper) as navigation_shipper_request_cnt
    from (select * from request_shippers_tab where is_navigation_shipper = 1)
    group by 1, 2, 3, 4
)

, navigation_shipper_distance_tab as (
    select 
        *
        , row_number() OVER (PARTITION BY shipper_id ORDER BY create_time ASC) AS row_number
        , ST_Distance(to_spherical_geography(ST_Point(lon, lat)), to_spherical_geography(ST_Point(pin_lon, pin_lat))) as navigation_distance
    from request_shippers_tab
    where is_navigation_shipper = 1
)

------------------------------------------------------response------------------------------------------------------

, response_tab AS(
    SELECT 
        id
        , city_id
        , grass_date
        , create_time
        , request_id
        , order_id
        , 'single' as assign_type
        , cast(json_extract(value, '$.shipper_id') as bigint ) AS shipper_id
        , CAST(json_extract(value, '$.driving_distance') AS double) AS driving_distance
        , CAST(json_extract(value, '$.flying_distance') AS double) AS flying_distance       
        , CAST(json_extract(value, '$.is_navigation') AS boolean) AS is_navigation
        , CAST(json_extract(value, '$.is_one_order_mode') AS boolean) AS is_one_order_mode
    FROM navigation_request_tab
    CROSS JOIN UNNEST(cast(ds_response_results as map<varchar,json>)) AS x(order_id, value_list)
    CROSS JOIN UNNEST(cast(value_list as array<json>)) AS a(value)

    union all 

    SELECT 
        id
        , city_id
        , grass_date
        , create_time
        , request_id
        , order_id
        , 'stack' as assign_type
        , cast(json_extract(value, '$.shipper_id') as bigint ) AS shipper_id
        , 0 AS driving_distance
        , 0 AS flying_distance       
        , CAST(json_extract(value, '$.is_navigation') AS boolean) AS is_navigation
        , CAST(json_extract(value, '$.is_one_order_mode') AS boolean) AS is_one_order_mode
    FROM navigation_stack_request_tab
    CROSS JOIN UNNEST(cast(ds_response_results as map<varchar,json>)) AS x(order_id, value_list)
    CROSS JOIN UNNEST(cast(value_list as array<json>)) AS a(value)
)

, navigation_shipper_response_mode_tab as (
    select 
        grass_date
        , city_id
        , shipper_id
        , is_one_order_mode
        , create_time - navigation_timestamp as assigning_time
    from (
        select 
            t1.*
            , row_number() OVER (PARTITION BY t3.navigation_timestamp ORDER BY t1.create_time ASC) AS row_number 
            , t3.navigation_timestamp
        from response_tab t1
        left join request_shippers_tab t3 on t1.request_id = t3.request_id and t1.shipper_id = t3.shipper_id
        where is_navigation
    )
    where row_number=1
)

, navigation_pair_tab as (
    select 
        *
        , row_number() OVER (PARTITION BY shipper_id ORDER BY create_time DESC) AS shipper_row_number
        , row_number() OVER (PARTITION BY shipper_id, navigation_timestamp ORDER BY create_time DESC) AS times_row_number
        , if(to_pin_distance <= pin_range, 1, 0) as is_navigated_succeed
    from (
        select 
            t1.*
            , t2.ref_order_id
            , t2.p_lat
            , t2.p_lon
            , t2.d_lat
            , t2.d_lon
            , t3.pin_lat
            , t3.pin_lon
            , t3.pin_range
            , t3.navigation_timestamp
            , ST_Distance(to_spherical_geography(ST_Point(t2.d_lon, t2.d_lat)), to_spherical_geography(ST_Point(t3.pin_lon, t3.pin_lat))) as to_pin_distance
            , t5.final_delivered_time
            , case when t5.final_delivered_time is not null and t5.final_delivered_time - t3.navigation_timestamp > 0 then t5.final_delivered_time - t3.navigation_timestamp
                else null end as navigation_duration
            , case when t5.is_quit=0 and t6.is_denied=0 and t6.is_ignore=0 then t5.is_delievered
                when t5.is_quit=1 or t6.is_denied=1 or t6.is_ignore=1 then 0
                else t5.is_delievered end as is_delievered
            , t5.is_quit
            , t6.is_denied
            , t6.is_ignore
        from response_tab t1 
        left join request_orders_tab t2 on t1.request_id = t2.request_id and t1.assign_type = t2.assign_type and t1.order_id = t2.order_id
        left join request_shippers_tab t3 on t1.request_id = t3.request_id and t1.shipper_id = t3.shipper_id
        left join basic_tab t4 on t2.ref_order_id = t4.order_id and t1.shipper_id = t4.shipper_uid
        left join basic_tab t5 on t2.ref_order_id = t5.order_id
        left join assign_tab t6 on t2.ref_order_id = t6.order_id and t1.shipper_id = t6.shipper_uid
        where t1.is_navigation
    )
)

------------------------------------------------------shipper report------------------------------------------------------

, driver_city_tab as (
    select 
        grass_date
        , shipper_id
        , MAX_BY(city_id, city_id_cnt) as city_id
    from (
        select 
            grass_date
            , shipper_id
            , city_id
            , count(city_id) as city_id_cnt
        from request_shippers_tab
        group by 1, 2, 3
    )
    group by 1, 2
)

, shipper_report_tab as (
    select 
        *
    from (
        select 
            t1.*
            , t2.city_id
        from (
            select 
                uid as shipper_uid
                , total_online_seconds
                , total_work_seconds
                , total_work_distance
                , total_completed_order - total_quit_order_by_shipper as total_completed_order
                , report_date
                , date_format(from_unixtime(report_date, 7, 0), '%Y-%m-%d') as report_date_format
            from shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live
            where date_format(from_unixtime(report_date, 7, 0), '%Y-%m-%d') >= '${start_date}'
            and date_format(from_unixtime(report_date, 7, 0), '%Y-%m-%d') <= '${end_date}'
        ) t1
        left join driver_city_tab t2 on t1.shipper_uid = t2.shipper_id and t1.report_date_format = t2.grass_date
    )
)

, navigation_shipper_report_tab as (
    select 
        t1.*
        , t2.total_online_seconds / 3600.0 as total_online_hours
        , t2.total_work_seconds / 3600.0 as total_work_hours
        , t2.total_completed_order
    from (select * from request_shippers_tab where is_navigation_shipper = 1) t1
    left join shipper_report_tab t2 on t1.grass_date = t2.report_date_format and t1.shipper_id = t2.shipper_uid
)

------------------------------------------------------metrics------------------------------------------------------

-----------------------------basic-----------------------------
, basic_metrics_tab as (
    select 
        grass_date
        , count(distinct order_id) as ado
        , 1.0000 * count(distinct case when is_net = 1 then order_id end) / count(distinct order_id) as g2n
        , 1.0 * sum(case when is_net = 1 and ata is not null and ata > 0 then ata end) / count(case when is_net = 1 and ata is not null and ata > 0 then ata end) as ata
    from basic_tab where 1=1 
    -- and city_id not in (217, 218)
    and city_id not in (217)
    group by 1
)

, online_drivers_tab as (
    select grass_date, count(distinct shipper_id) as online_drivers from driver_city_tab where 1=1 
    -- and city_id not in (217, 218)
    and city_id not in (217)
    group by 1
)
-----------------------------driver behavior-----------------------------
, navigation_drivers_tab as (
    select grass_date, count(distinct shipper_id) as navigation_drivers from navigation_shipper_request_cnt_tab
    group by 1
)

, navigation_times_tab as (
    select grass_date, count(shipper_id) as navigation_times from navigation_shipper_request_times_cnt_tab
    group by 1
)

, navigation_distance_tab as (
    select grass_date, cast(avg(navigation_distance) as int) as navigation_distance, cast(min(navigation_distance) as int) as min_navigation_distance, cast(max(navigation_distance) as int) as max_navigation_distance from (select * from navigation_shipper_distance_tab where row_number=1)
    group by 1
)

, navigation_report_tab as (
    select grass_date, avg(total_online_hours) as navigation_online_hours, avg(total_work_hours) as navigation_work_hours, 1.0 * sum(total_completed_order) / count(total_completed_order) as orders_per_navigation_driver from navigation_shipper_report_tab
    group by 1
)

-----------------------------delivery performance-----------------------------
, navigation_mode_cnt_tab as (
=    group by 1,2
)

, navigated_succeed_drivers_tab as (
    select grass_date, sum(is_navigated_succeed) as navigated_succeed_drivers from (select * from navigation_pair_tab where shipper_row_number=1)
    group by 1
)

, navigated_succeed_times_tab as (
    select grass_date, sum(is_navigated_succeed) as navigated_succeed_times, 1.0 * sum(case when navigation_duration is not null then navigation_duration end) / count(case when navigation_duration is not null then navigation_duration end) as navigation_duration from (select * from navigation_pair_tab where times_row_number=1)
    group by 1
)

, navigation_orders_tab as (
    select grass_date, count(order_id) as navigation_orders, sum(is_delievered) as delivered_orders, sum(is_ignore) as ignore_orders, sum(is_quit) as quit_orders, sum(is_denied) as denied_orders from navigation_pair_tab
    group by 1
)

, navigation_assigning_time_tab as (
    select grass_date, 1.0 * sum(assigning_time) / count(assigning_time) as assigning_time from navigation_shipper_response_mode_tab
    group by 1
)

, navigation_pickup_distance_tab as (
    select grass_date, 1.0 * sum(driving_distance) / count(driving_distance) as navigation_pickup_distance from (select * from navigation_pair_tab where assign_type = 'single')
    group by 1
)
-----------------------------navigation metics-----------------------------
, metrics_tab as (
    select 
        t13.*
        , t1.online_drivers
        , t2.navigation_drivers
        , t3.navigation_times
        , t4.navigation_distance
        , t4.min_navigation_distance
        , t4.max_navigation_distance
        , '' as navigation_off_drivers
        , 1.0000 * t2.navigation_drivers / t1.online_drivers as navigation_driver_rate
        , t5.navigation_online_hours
        , t5.navigation_work_hours
        , t5.orders_per_navigation_driver
        , t6.mode_cnt as one_mode_drivers
        , t7.mode_cnt as multi_mode_drivers
        , t8.navigated_succeed_drivers
        , t9.navigated_succeed_times
        , t10.navigation_orders
        , t10.delivered_orders
        , t10.ignore_orders
        , t10.quit_orders
        , t10.denied_orders
        , t9.navigation_duration
        , 1.0000 * t8.navigated_succeed_drivers / t2.navigation_drivers as navigation_succeed_driver_rate
        , 1.0000 * t9.navigated_succeed_times / t3.navigation_times as navigation_succeed_times_rate
        , 1.0000 * t10.navigation_orders / t13.ado as navigation_order_rate
        , t11.assigning_time
        , t12.navigation_pickup_distance
    from online_drivers_tab t1
    left join navigation_drivers_tab t2 on t1.grass_date = t2.grass_date
    left join navigation_times_tab t3 on t1.grass_date = t3.grass_date
    left join navigation_distance_tab t4 on t1.grass_date = t4.grass_date
    left join navigation_report_tab t5 on t1.grass_date = t5.grass_date
    left join navigation_mode_cnt_tab t6 on t1.grass_date = t6.grass_date and t6.is_one_order_mode
    left join navigation_mode_cnt_tab t7 on t1.grass_date = t7.grass_date and not t7.is_one_order_mode
    left join navigated_succeed_drivers_tab t8 on t1.grass_date = t8.grass_date
    left join navigated_succeed_times_tab t9 on t1.grass_date = t9.grass_date
    left join navigation_orders_tab t10 on t1.grass_date = t10.grass_date
    left join navigation_assigning_time_tab t11 on t1.grass_date = t11.grass_date
    left join navigation_pickup_distance_tab t12 on t1.grass_date = t12.grass_date
    left join basic_metrics_tab t13 on t1.grass_date = t13.grass_date
)

select * from metrics_tab