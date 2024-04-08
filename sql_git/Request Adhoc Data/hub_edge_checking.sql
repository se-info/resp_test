WITH raw AS 
(SELECT 
         id
        ,order_code 
        ,distance
        ,hub_id 
        ,ARRAY_MIN(ARRAY_AGG(hub_edge_distance)) AS min_edge
        ,ARRAY_AGG(hub_edge_distance) AS edge_info 
         
FROM        
(SELECT 
        raw.id
       ,raw.order_code 
       ,doet.hub_id
       ,doet.pick_hub_id
       ,doet.drop_hub_id
       ,raw.distance
       ,raw.city_name
       ,raw.shipper_id
       ,raw.created_date
       ,raw.drop_latitude
       ,raw.drop_longitude
       ,"great_circle_distance"(raw.drop_latitude,raw.drop_longitude,
                                 CAST(hi.hub_lat AS DOUBLE),CAST(hi.hub_long AS DOUBLE)) AS hub_edge_distance  

FROM dev_vnfdbi_opsndrivers.phong_raw_order_v2 raw 

LEFT JOIN 
    (SELECT order_id,JSON_EXTRACT(order_data,'$.hub_id') AS hub_id,JSON_EXTRACT(order_data,'$.pick_hub_id') AS pick_hub_id,JSON_EXTRACT(order_data,'$.drop_hub_id') AS drop_hub_id
     FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da
     WHERE DATE(dt) = current_date - interval '1' day
    ) doet on doet.order_id = raw.delivery_id

LEFT JOIN 
(SELECT 
       raw.id 
      ,raw.hub_name 
      ,CAST(t.points AS ARRAY<JSON>)[1] AS hub_lat
      ,CAST(t.points AS ARRAY<JSON>)[2] AS hub_long

FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live raw 

CROSS JOIN UNNEST (CAST(JSON_EXTRACT(raw.extra_data,'$.geo_data.points') AS ARRAY<JSON>)) AS t(points)
) hi on hi.id = CAST(doet.pick_hub_id AS BIGINT)

WHERE 1 = 1 
AND raw.order_type = 0 
AND raw.created_date between current_date - interval '1' day and current_date - interval '1' day
AND CAST(doet.hub_id AS BIGINT) > 0 
-- AND raw.distance > 3.6
AND doet.pick_hub_id <> doet.drop_hub_id
)
GROUP BY 1,2,3,4
)
SELECT * FROM raw WHERE (distance > 3.6 OR min_edge > 1.2)