WITH raw AS
(SELECT 
        raw.id
       ,raw.order_code 
       ,raw.hub_id
       ,raw.pick_hub_id
       ,raw.drop_hub_id
       ,raw.distance
       ,raw.city_name
       ,raw.shipper_id
       ,raw.created_date
       ,raw.drop_latitude
       ,raw.drop_longitude
       ,raw.driver_policy
       ,"great_circle_distance"(raw.drop_latitude,raw.drop_longitude,
                                 CAST(hi.hub_lat AS DOUBLE),CAST(hi.hub_long AS DOUBLE)) AS hub_edge_distance  

FROM dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

LEFT JOIN 
(SELECT 
       raw.id 
      ,raw.hub_name 
      ,CAST(t.points AS ARRAY<JSON>)[1] AS hub_lat
      ,CAST(t.points AS ARRAY<JSON>)[2] AS hub_long

FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live raw  

CROSS JOIN UNNEST (CAST(JSON_EXTRACT(raw.extra_data,'$.geo_data.points') AS ARRAY<JSON>)) AS t(points)
) hi on hi.id = CAST(raw.pick_hub_id AS BIGINT)

WHERE 1 = 1 
AND raw.order_status = 'Delivered'
AND raw.order_type = 0 
AND raw.created_date >= DATE'2024-03-01'
AND CAST(raw.pick_hub_id AS BIGINT) > 0 
AND raw.city_name in ('HCM City','Ha Noi City')
)
,f AS
(SELECT 
         DATE_TRUNC('month',created_date) AS month_,   
         created_date,
         id,
         order_code ,
         distance,
         hub_id,
         pick_hub_id,
         drop_hub_id,
         driver_policy,
         IF(pick_hub_id<>drop_hub_id,1,0) AS is_out_hub,
         ARRAY_MIN(ARRAY_AGG(hub_edge_distance)) AS min_edge
        --  ARRAY_AGG(hub_edge_distance) AS edge_info 
         
FROM raw 

GROUP BY 1,2,3,4,5,6,7,8,9,10
)
SELECT 
        month_,
        COUNT(DISTINCT order_code)*1.0000/COUNT(DISTINCT created_date) AS total_order,
        COUNT(DISTINCT CASE WHEN current_qualified = 1 THEN order_code ELSE NULL END)*1.0000
            /COUNT(DISTINCT created_date) AS total_hub_order_current,
        COUNT(DISTINCT CASE WHEN current_qualified = 1 AND is_out_hub = 1 THEN order_code ELSE NULL END)*1.0000
            /COUNT(DISTINCT created_date) AS total_out_hub_order_current,

        COUNT(DISTINCT CASE WHEN "distance < 4 and border < 1.8" = 1 THEN order_code ELSE NULL END)*1.0000
            /COUNT(DISTINCT created_date) AS total_hub_order_estimation,
        COUNT(DISTINCT CASE WHEN "distance < 4 and border < 1.8" = 1 AND is_out_hub = 1 THEN order_code ELSE NULL END)*1.0000
            /COUNT(DISTINCT created_date) AS total_out_hub_order_estimation,

        COUNT(DISTINCT CASE WHEN "distance < 4 and border < 1.8" = 1 AND distance > 3.6 AND min_edge > 1.8 THEN order_code ELSE NULL END)*1.0000
            /COUNT(DISTINCT created_date) AS "out hub order distance > 3.6 and border > 1.8",
        COUNT(DISTINCT CASE WHEN "distance < 4 and border < 1.8" = 1 AND distance > 4 THEN order_code ELSE NULL END)*1.0000
            /COUNT(DISTINCT created_date) AS "out hub order distance > 4"


FROM
(SELECT 
        month_,
        created_date,
        CASE 
        WHEN hub_id > 0 THEN 1
        WHEN driver_policy = 2 THEN 1
        WHEN (pick_hub_id > 0 AND distance < 3.6 AND min_edge < 1.5) THEN 1
        ELSE 0 END AS current_qualified,

        CASE 
        WHEN hub_id > 0 THEN 1
        WHEN driver_policy = 2 THEN 1
        WHEN (pick_hub_id > 0 AND distance < 4 AND min_edge < 1.8) THEN 1
        ELSE 0 END AS "distance < 4 and border < 1.8",
        order_code,
        min_edge,
        distance,
        is_out_hub
FROM f)

GROUP BY 1


