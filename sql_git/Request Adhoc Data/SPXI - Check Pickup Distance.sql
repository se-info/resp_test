WITH assignment AS
(SELECT * FROM dev_vnfdbi_opsndrivers.phong_temp_assign WHERE status in (3,4))
,raw AS 
(SELECT 
       log.order_id AS ref_order_id
      ,dot.ref_order_code 
      ,log.order_type
      ,'order_shopee' AS source
      ,city.name_en AS city_name  
      ,FROM_UNIXTIME(log.create_time - 3600) AS pick_timestamp
      ,SPLIT(log.location,',') AS driver_pickup_locations
      ,dot.sender_location
      ,dot.uid AS shipper_id
    --   ,typeof(SPLIT(log.location,',')[1])
      ,"great_circle_distance"(
        CAST(SPLIT(log.location,',')[1] AS DOUBLE) ,CAST(SPLIT(log.location,',')[2] AS DOUBLE)
        ,CAST(dot.sender_location[1] AS DOUBLE),CAST(dot.sender_location[2] AS DOUBLE)  
        )*1000 AS pick_to_sender_circle_distance_meter
      ,CASE 
            WHEN dot.group_id > 0 AND sa.order_type != 'Group' THEN 'stack'
            WHEN dot.group_id > 0 AND sa.order_type = 'Group' THEN 'group'
            ELSE 'single' END AS assign_type                      

FROM shopeefood.foody_partner_db__order_shipper_status_log_tab__reg_daily_s0_live log

LEFT JOIN 
(SELECT 
      *
     ,(ARRAY[pick_latitude,pick_longitude]) AS sender_location
FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da
where date(dt) = current_date - interval '1' day
) dot on dot.ref_order_id = log.order_id 
      and dot.ref_order_category = log.order_type    

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
    on city.id = dot.pick_city_id
    and city.country_id = 86

LEFT JOIN assignment sa 
    on sa.ref_order_id = log.order_id
    and sa.order_category = log.order_type

LEFT JOIN assignment sa_filter
    on  sa.ref_order_id = sa_filter.ref_order_id          
    and sa.order_category = sa_filter.order_category 
    and sa.create_time < sa_filter.create_time

WHERE 1 = 1 
AND sa_filter.order_id IS NULL
AND log.order_type = 6
AND log.status = 8

)
SELECT *
FROM raw 
WHERE 1 = 1
AND DATE(pick_timestamp) between current_date - interval '3' day and current_date - interval '1' day
AND pick_to_sender_circle_distance_meter > 300
