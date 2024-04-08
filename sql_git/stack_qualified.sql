WITH raw AS 
(SELECT 
        id,
        grass_date,
        city_id,
        FROM_UNIXTIME(create_time - 3600) AS created,
        JSON_EXTRACT(processing_info,'$.task') AS task,
        JSON_EXTRACT(processing_info,'$.ds_stack_request.stacking_orders') AS ds_stack_request_orders

FROM shopeefood.foody_partner_archive_db__order_assign_shipper_batch_processing_log_tab_di
WHERE JSON_EXTRACT(processing_info,'$.ds_stack_request.stacking_orders') IS NOT NULL
)
,stack_request AS 
(SELECT 
        raw.id,
        raw.grass_date,
        i.order_id,
        raw.city_id,
        i.info,
        HOUR(raw.created) AS hourly,
        doet.hub_id,
        doet.driver_policy,

FROM raw 
CROSS JOIN UNNEST (CAST(ds_stack_request_orders AS map<bigint,json>)) AS i(order_id,info)
LEFT JOIN 
(SELECT     
        order_id,
        CAST(JSON_EXTRACT(order_data,'$.shipper_policy.type') AS BIGINT) AS driver_policy,
        COALESCE(CAST(json_extract(order_data,'$.hub_id') as BIGINT),0) as hub_id,
        COALESCE(CAST(json_extract(order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id,
        COALESCE(CAST(json_extract(order_data,'$.drop_hub_id') as BIGINT ),0) as drop_hub_id,
        order_data

 FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da 
 WHERE date(dt) = current_date - interval '1' day
 ) doet 
    on doet.order_id = i.order_id
)
SELECT
        MONTH(grass_date) AS monthly,
        city_id,
        hourly,
        COUNT(DISTINCT order_id)/CAST(COUNT(DISTINCT grass_date) AS DOUBLE) AS order_being_stack

FROM stack_request 

WHERE CAST(grass_date AS DATE) BETWEEN date'2023-07-01' AND DATE'2023-09-18'
AND city_id = 217
GROUP BY 1,2,3









