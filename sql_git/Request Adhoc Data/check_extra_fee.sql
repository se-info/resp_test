WITH raw AS 
(SELECT 
        ogi.id AS group_id
       ,ogi.group_code 
       ,ogi.ref_order_category
       ,ogm.ref_order_id
       ,dot.pick_city_id
       ,CAST(dot.pick_latitude AS DECIMAL(7,4)) AS pick_latitude
       ,CAST(dot.pick_longitude AS DECIMAL(7,4)) AS pick_longitude
       ,CAST(dot.drop_latitude AS DECIMAL(7,4)) AS drop_latitude
       ,CAST(dot.drop_longitude AS DECIMAL(7,4)) AS drop_longitude
       ,dot.delivery_cost/CAST(100 AS DOUBLE) AS single_fee 
       ,ogm.mapping_status
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_a') AS DOUBLE) AS rate_a 
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_b') AS DOUBLE) AS rate_b
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.unit_fee') AS DOUBLE) AS unit_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.surge_rate') AS DOUBLE) AS surge_rate
       ,CEILING(CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee') AS DOUBLE)) AS min_fee
       ,CEILING(CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.per_km') AS DOUBLE) * 
       CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.surge_rate') AS DOUBLE) * (ogi.distance/cast(100000 as double))) as total_group_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE) AS extra_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.re') AS DOUBLE) AS re_stack
       ,DATE(FROM_UNIXTIME(ogi.create_time - 3600)) AS group_created_date
       ,ogi.ship_fee/CAST(100 AS DOUBLE) AS original_fee    
       ,ROW_NUMBER()OVER(PARTITION BY ogi.id order by ogm.ref_order_id desc) AS rank   

FROM (SELECT distance/cast(100000 as double),* FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da 
      WHERE DATE(dt) = current_date - interval '1' day

      ) ogi

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da 
           WHERE DATE(dt) = current_date - interval '1' day

           ) ogm 
    on ogm.group_id = ogi.id 
    and ogm.ref_order_category = ogi.ref_order_category

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = ogm.ref_order_id
    and dot.ref_order_category = ogm.ref_order_category
)
,check AS   
(SELECT  
        group_created_date
       ,group_code
       ,min_fee
       ,ref_order_category
       ,total_group_fee
       ,original_fee
       ,original_fee - GREATEST(min_fee,total_group_fee) AS extra_fee_cal 
       ,IF(ref_order_category = 0, 1000 , 500  ) * 2 * (COUNT(DISTINCT ref_order_id) - 1) AS current_extra_fee
       ,COUNT(DISTINCT ref_order_id) AS total_order_in_group
       ,ARRAY_AGG(DISTINCT ref_order_id) AS order_ext
                                               
       ,CARDINALITY(array_agg(pick_latitude)) AS count_pick_lat
       ,CARDINALITY(array_agg(DISTINCT pick_latitude)) AS count_pick_lat_unique
       ,CARDINALITY(array_agg(pick_longitude)) AS count_pick_long
       ,CARDINALITY(array_agg(DISTINCT pick_longitude)) AS count_pick_long_unique

                    
       ,CARDINALITY(array_agg(drop_latitude)) AS count_drop_lat
       ,CARDINALITY(array_agg(DISTINCT drop_latitude)) AS count_drop_lat_unique
       ,CARDINALITY(array_agg(drop_longitude)) AS count_drop_long
       ,CARDINALITY(array_agg(DISTINCT drop_longitude)) AS count_drop_long_unique

FROM raw 
WHERE mapping_status = 11 

GROUP BY 1,2,3,4,5,6,7,ref_order_category,extra_fee
)
,summary AS 
(SELECT 
        group_created_date
       ,group_code 
       ,order_ext
       ,CASE 
            WHEN total_order_in_group > 1 THEN ceiling(extra_fee_cal) ELSE 0 END AS extra_fee_cal 
       ,current_extra_fee AS system_extra_fee  
    --    ,count_pick_lat_unique
    --    ,count_pick_long_unique
    --    ,count_drop_lat_unique
    --    ,count_drop_long_unique
       ,(CASE 
            WHEN count_pick_lat_unique = count_pick_long_unique THEN IF(ref_order_category = 0, 1000 , 500  ) * (count_pick_lat_unique - 1)
            WHEN count_pick_lat_unique != count_pick_long_unique THEN IF(ref_order_category = 0, 1000 , 500  ) *(GREATEST(count_pick_lat_unique,count_pick_long_unique) -1)
            END) +
        (CASE 
            WHEN count_drop_lat_unique = count_drop_long_unique THEN IF(ref_order_category = 0, 1000 , 500  ) * (count_drop_lat_unique - 1)
            WHEN count_drop_lat_unique != count_drop_long_unique THEN IF(ref_order_category = 0, 1000 , 500  ) * (GREATEST(count_drop_lat_unique,count_drop_long_unique) -1)
            END) AS new_extra_fee    
       ,original_fee,min_fee,total_group_fee 
FROM check

                                                                  
              
                                                                   
)


SELECT
         group_created_date AS created_date
        ,SUM(system_extra_fee) AS current_extra_fee
        ,SUM(new_extra_fee) AS new_extra_fee 

FROM summary

WHERE group_created_date BETWEEN DATE'2023-03-01' AND DATE'2023-03-31'
GROUP BY 1 