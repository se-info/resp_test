WITH group_info_tab as 
(SELECT 
        id as group_id
        , CAST(JSON_EXTRACT(t.route,'$.order_id') AS INT) AS delivery_id
        , CAST(JSON_EXTRACT(t.route,'$.is_pick') AS VARCHAR) AS is_pick
        , group_code
        , t.route
        , ref_order_category
        , cast(json_extract(extra_data, '$.distance_matrix.data') as ARRAY(ARRAY(integer))) as distance_matrix
        , cast(json_extract(extra_data, '$.distance_matrix.mapping') as ARRAY(integer)) as mapping        
        , distance * 1.00 / 100 as group_distance
        , cast(json_extract(extra_data, '$.re') as double) AS re 
        , cast(json_extract(extra_data, '$.pick_city_id') as int) AS city_id
        , json_array_length(json_extract(extra_data, '$.distance_matrix.mapping')) / 2 as group_order_cnt
        , ship_fee * 1.00 / 100 / (json_array_length(json_extract(extra_data, '$.distance_matrix.mapping'))/2) as group_fee_per_order
        , extra_data
        , DATE(FROM_UNIXTIME(create_time - 3600)) AS created_date
       
FROM shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live raw 

CROSS JOIN UNNEST (CAST(JSON_EXTRACT(raw.extra_data,'$.route') AS ARRAY<JSON>) ) AS t(route)

WHERE ref_order_category != 0
)
,summary AS 
(select 
         row_number()over(partition by group_code) AS row_num
        ,delivery_id  
        ,is_pick
        ,group_id 
        ,group_code
        ,group_distance
        ,mapping
        ,distance_matrix
        ,ref_order_category
        ,created_date
        -- ,array_position(mapping,IF(is_pick = 'true',-1*delivery_id,delivery_id)) as check_index
        ,CASE WHEN ref_order_category = 0 THEN 'order_delivery' ELSE 'order_spxi' END AS source
        ,re

from group_info_tab
WHERE created_date BETWEEN current_date - interval '30' day and current_date - interval '1' day
AND group_order_cnt > 1
)
,final AS 
(SELECT *,distance_matrix[check1][check2] AS distance_check
FROM
(SELECT 
        t1.row_num
       ,t1.created_date 
       ,t1.ref_order_category         
       ,t1.group_code 
       ,COALESCE(t2.row_num,0) AS row_num_map
       ,t1.delivery_id
       ,raw.order_code 
    --    ,t1.check_index
       ,t1.is_pick
       ,t1.group_distance
       ,ARRAY_POSITION(t1.mapping,IF(t1.is_pick = 'true',-1*t1.delivery_id,t1.delivery_id)) as check1
       ,ARRAY_POSITION(t1.mapping,IF(t2.is_pick = 'true',-1*t2.delivery_id,t2.delivery_id)) as check2
       ,COALESCE(t2.delivery_id,0) AS delivery_id_map  
    --    ,COALESCE(t2.check_index,0) AS check_index_map
       ,COALESCE(t2.is_pick,'0') AS is_pick_map
       ,t1.distance_matrix
       ,t1.re
       ,raw.distance
       ,raw.order_status
    --    ,CASE 
    --         WHEN COALESCE(t2.row_num,0) > 0 
    --         THEN COALESCE(t1.distance_matrix[
    --                        ARRAY_POSITION(t1.mapping,IF(t1.is_pick = 'true',-1*t1.delivery_id,t1.delivery_id))
    --                        ]
    --                        [
    --                         ARRAY_POSITION(t1.mapping,IF(t2.is_pick = 'true',-1*t2.delivery_id,t2.delivery_id))
    --                        ],0) ELSE 0 END AS distance_check


FROM summary t1 

LEFT JOIN summary t2 
    on t1.group_id = t2.group_id 
    and t1.row_num + 1 = t2.row_num 

LEFT JOIN dev_vnfdbi_opsndrivers.phong_raw_order_v2 raw 
    on t1.delivery_id = raw.delivery_id 
    and t1.ref_order_category = raw.order_type

WHERE t2.group_id IS NOT NULL 
ORDER BY 1 asc )
where check1 > 0 and check2 > 0
)
-- SELECT 
--          created_date
--         ,cnt_group_order
--         ,distance_range
--         ,SUM(system_distance)/CAST(COUNT(DISTINCT group_code) AS DOUBLE) AS avg_group_distance
--         ,SUM(distance_between_pickup_point)/CAST(COUNT(DISTINCT group_code) AS DOUBLE) AS avg_distance_between_pickup_point
--         ,COUNT(DISTINCT group_code) AS cnt_group 
--         ,SUM(cnt_group_order) AS cnt_order_in_group

SELECT *
FROM
(SELECT
       created_date
      ,group_code
      ,re AS re_system
      ,CASE 
             WHEN re <= 1.05 then '1. <= 1.05'
             WHEN re <= 1.1 then '2. <= 1.1'
             WHEN re <= 1.15 then '3. <= 1.15'
             WHEN re <= 1.2 then '4. <= 1.2'
             WHEN re <= 1.25 then '5. <= 1.25'
             WHEN re <= 1.3 then '6. <= 1.3'
             WHEN re <= 1.35 then '7. <= 1.35'
             WHEN re <= 1.4 then '8. <= 1.4'
             WHEN re <= 1.45 then '9. <= 1.45'
             WHEN re <= 1.5 then '10. <= 1.5'
             WHEN re > 1.5 then '11. > 1.5'
             END AS re_range 
      ,CASE 
            WHEN (ROUND(group_distance,0)/CAST(1000 AS DOUBLE)) <= 3 THEN '1. 0 - 3km'
            WHEN (ROUND(group_distance,0)/CAST(1000 AS DOUBLE)) <= 5 THEN '2. 3.1 - 5km'
            WHEN (ROUND(group_distance,0)/CAST(1000 AS DOUBLE)) <= 10 THEN '3. 5.1 - 10km'
            WHEN (ROUND(group_distance,0)/CAST(1000 AS DOUBLE)) > 10 THEN '4. ++10km'
            END AS distance_range
      ,ROUND(group_distance,0)/CAST(1000 AS DOUBLE) AS system_distance 
      ,CASE WHEN CARDINALITY(FILTER(ARRAY_AGG(DISTINCT CASE WHEN order_status IN ('Delivered','Returned','Pickup Failed') THEN order_code ELSE NULL END),x -> x is not null)) > 1 
            THEN SUM(distance)/(ROUND(group_distance,0)/CAST(1000 AS DOUBLE)) ELSE 0 END AS re_cal
      ,FILTER(ARRAY_AGG(DISTINCT CASE WHEN order_status IN ('Delivered','Returned','Pickup Failed') THEN order_code ELSE NULL END),x -> x is not null) AS order_code_ext
      ,CARDINALITY(FILTER(ARRAY_AGG(DISTINCT CASE WHEN order_status IN ('Delivered','Returned','Pickup Failed') THEN order_code ELSE NULL END),x -> x is not null)) AS cnt_group_order
      ,SUM(CASE WHEN is_pick = 'true' and is_pick_map = 'true' THEN distance_check ELSE NULL END)/CAST(1000 AS DOUBLE) AS distance_between_pickup_point  
      ,SUM(distance_check)/CAST(1000 AS DOUBLE) AS calculated_distance




FROM final 

WHERE 1 = 1 
AND created_date = current_date - interval '1' day 

GROUP BY 1,2,3,4,5,6
)
WHERE cnt_group_order > 0
AND distance_between_pickup_point > 0
AND system_distance > 0 
-- GROUP BY 1,2,3