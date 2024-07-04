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
AND raw.created_date between current_date - interval '15' day and current_date - interval '1' day
AND CAST(raw.pick_hub_id AS BIGINT) > 0 
AND raw.city_name in ('HCM City','Ha Noi City')
)
SELECT 
        CASE 
        WHEN min_edge <= 1.5 THEN '1. Current'
        WHEN min_edge <= 1.6 THEN '2. 1.5 - 1.6'
        WHEN min_edge <= 1.7 THEN '3. 1.6 - 1.7'
        WHEN min_edge <= 1.8 THEN '4. 1.7 - 1.8'
        WHEN min_edge <= 1.9 THEN '5. 1.8 - 1.9'
        WHEN min_edge <= 2.0 THEN '6. 1.9 - 2.0' 
        ELSE '7. ++2'
        END AS border_distance_range,
        CASE 
        WHEN distance <= 3.6 THEN '1. Current'
        WHEN distance <= 3.7 THEN '2. 3.6 - 3.7'
        WHEN distance <= 3.8 THEN '3. 3.7 - 3.8'
        WHEN distance <= 3.9 THEN '4. 3.8 - 3.9'
        WHEN distance <= 4.0 THEN '5. 3.9 - 4.0'
        ELSE '6. ++4' END AS order_distance_range,
        COUNT(DISTINCT order_code)/CAST(COUNT(DISTINCT created_date) AS DECIMAL(10,2)) AS total_order,

        AVG(distance) AS avg_order_distance,
        MAX(distance) AS max_order_distance,
        APPROX_PERCENTILE(distance,0.8) AS pct80th_order_distance,
        APPROX_PERCENTILE(distance,0.9) AS pct90th_order_distance,
        APPROX_PERCENTILE(distance,0.95) AS pct95th_order_distance,

        AVG(min_edge) AS avg_border_distance,
        MAX(min_edge) AS max_border_distance,
        APPROX_PERCENTILE(min_edge,0.8) AS pct80th_border_distance,
        APPROX_PERCENTILE(min_edge,0.9) AS pct90th_border_distance,
        APPROX_PERCENTILE(min_edge,0.95) AS pct95th_border_distance
FROM
(SELECT 
         created_date,
         id,
         order_code ,
         distance,
         hub_id,
         pick_hub_id,
         drop_hub_id,
         ARRAY_MIN(ARRAY_AGG(hub_edge_distance)) AS min_edge
        --  ARRAY_AGG(hub_edge_distance) AS edge_info 
         
FROM raw 

GROUP BY 1,2,3,4,5,6,7)
WHERE pick_hub_id > 0 
AND pick_hub_id != drop_hub_id
GROUP BY 1,2
