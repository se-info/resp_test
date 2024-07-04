WITH orig_tab AS (
    SELECT 
        id
        , city_id
        , processing_time
        , grass_date
        , create_time
        , date_format(FROM_UNIXTIME(create_time, 7, 0), '%Y-%m-%d %H:%i:%s') AS date_str
        
        , json_extract_scalar(processing_info, '$.ds_response["batch_id"]') AS batch_id
        , json_extract(processing_info, '$.ds_response["results"]') AS ds_response_results

        , json_extract(processing_info, '$.ds_request') AS ds_request
        , json_extract(processing_info, '$.ds_request["info"]') AS ds_request_info
        , json_extract(processing_info, '$.ds_request["order_shippers"]') AS ds_request_orders
        , json_extract(processing_info, '$.ds_request["shippers"]') AS ds_request_shippers
        -- , cast(json_extract(processing_info, '$.ds_request["order_shippers"]') as varchar) AS ds_request_order_string
        , json_extract(processing_info, '$.ds_request["vehicles"]') AS ds_request_vehicles
    from shopeefood.foody_partner_archive_db__order_assign_shipper_batch_processing_log_tab__reg_daily_s0_live
    WHERE not json_extract(processing_info, '$.ds_request["info"]') is null
    and CAST(grass_date AS DATE) between date'2023-08-01' - interval '7' day and date'2023-08-02' 
)

, response_parse_tab AS (
    SELECT 
        batch_id
        , city_id
        , grass_date
        , create_time
        , date_str
        , order_id
        , cast(split(order_id, '_')[2] as bigint) as ref_order_id
        , CAST(json_extract(value, '$.shipper_id') as bigint ) AS shipper_id
        , CAST(json_extract(value, '$.pred_accept_prob') AS double) AS pred_accept_prob
        , CAST(json_extract(value, '$.driving_distance') AS double) AS driving_distance
    FROM orig_tab
    CROSS JOIN UNNEST(cast(ds_response_results as map<varchar,json>)) AS x(order_id, value_list)
    CROSS JOIN UNNEST(cast(value_list as array<json>)) AS a(value)
)

, request_shipper_parse_tab AS (
    SELECT 
        batch_id
        , city_id
        , grass_date
        , shipper_id
        , CAST(json_extract(value, '$.onmap_status') AS int) AS onmap_status
        , CAST(json_extract(value, '$.lat') AS double) AS driver_lat
        , CAST(json_extract(value, '$.lon') AS double) AS driver_lon
    FROM orig_tab
    CROSS JOIN UNNEST(cast(ds_request_shippers as map<bigint,json>)) AS x(shipper_id, value)
)

, assign_result_tab AS (
    select 
        t1.*
        , t2.onmap_status
        , case when t2.onmap_status is not null and t2.onmap_status = 4 then 1 else 0 end as is_ca_driver
        , t2.driver_lat
        
        , t2.driver_lon
    from response_parse_tab t1
    left join request_shipper_parse_tab t2 
    on t1.grass_date = t2.grass_date and t1.city_id = t2.city_id and t1.batch_id = t2.batch_id and t1.shipper_id = t2.shipper_id
)


-- get ca_driver in assignment reqeust
-- select * from request_shipper_parse_tab where onmap_status = 4

-- get ca_driver assigned in assignment response
select grass_date,COUNT(shipper_id) as total_ca_driver from assign_result_tab where is_ca_driver = 1 and city_id = 217 group by 1 



