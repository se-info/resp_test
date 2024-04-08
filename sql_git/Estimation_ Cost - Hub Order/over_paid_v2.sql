WITH raw AS
(SELECT 
       dot.group_id
      ,dot.pick_city_id 
      ,DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date
      ,ogi.ship_fee/CAST(100 AS DOUBLE) AS group_fee
      ,ARRAY_JOIN(ARRAY_AGG(DISTINCT dot.ref_order_category),',') AS order_cate
      ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE) * 2 * GREATEST((CARDINALITY(ARRAY_AGG(dot.ref_order_code))-1),1) AS extra_fee
      ,CARDINALITY(ARRAY_AGG(dot.ref_order_code)) AS total_order_in_group       
      ,SUM(dot.delivery_cost/CAST(100 AS DOUBLE)) AS sum_single_fee 
      ,ARRAY_AGG(dot.ref_order_code) AS order_code_ext 
      ,ARRAY_AGG(dot.delivery_cost/CAST(100 AS DOUBLE)) AS single_fee_ext 
      ,((ogi.ship_fee/cast(100 as double)) - (CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE) * 2 * (CARDINALITY(ARRAY_AGG(dot.ref_order_code))-1))) 
        - SUM(dot.delivery_cost/CAST(100 AS DOUBLE)) AS gap_ship_fee
      ,SUM(dot.delivery_distance/CAST(1000 AS DOUBLE)) AS sum_single_distance            
      ,ogi.distance/CAST(100000 AS DOUBLE)  AS group_distance
      ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_a') AS DOUBLE) AS rate_a 
      ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_b') AS DOUBLE) AS rate_b
      ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.unit_fee') AS DOUBLE) AS unit_fee
      ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.surge_rate') AS DOUBLE) AS surge_rate
      ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee') AS DOUBLE) AS min_fee
      ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE) AS extra_fee
      ,CAST(JSON_EXTRACT(ogi.extra_data,'$.re') AS DOUBLE) AS re_stack           


FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da  WHERE date(dt) = current_date - interval '1' day) dot 

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da  WHERE date(dt) = current_date - interval '1' day) ogi 
    on ogi.id = dot.group_id

WHERE DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN DATE'2023-04-05' - INTERVAL '7' DAY AND DATE'2023-04-05' - INTERVAL '1' DAY 

AND dot.group_id > 0 

GROUP BY 1,2,3,4,ogi.distance,ogi.extra_data,ogi.ship_fee       
)
,summary AS 
(SELECT
         report_date
        ,group_id 
        ,order_cate
        ,pick_city_id
        ,group_distance - sum_single_distance AS gap_distance
        ,(group_distance - sum_single_distance)/CAST(sum_single_distance AS DOUBLE) AS pct_gap_distance
        ,CASE 
             WHEN (group_distance - sum_single_distance) <= 1 THEN '1. 0 - 1km'
             WHEN (group_distance - sum_single_distance) <= 2 THEN '2. 1 - 2km'
             WHEN (group_distance - sum_single_distance) <= 3 THEN '3. 2 - 3km'
             WHEN (group_distance - sum_single_distance) <= 5 THEN '4. 3 - 5km'
             WHEN (group_distance - sum_single_distance) <= 10 THEN '5. 5 - 10km'
             WHEN (group_distance - sum_single_distance) <= 15 THEN '6. 10 - 15km'
             WHEN (group_distance - sum_single_distance) > 15 THEN '7. ++15km'
             END AS distance_gap_range
        ,CASE 
             WHEN (group_distance - sum_single_distance)/CAST(sum_single_distance AS DOUBLE) <= 10 THEN '1. 0 - 10%'
             WHEN (group_distance - sum_single_distance)/CAST(sum_single_distance AS DOUBLE) <= 30 THEN '2. 10 - 30%'
             WHEN (group_distance - sum_single_distance)/CAST(sum_single_distance AS DOUBLE) > 30 THEN '3. ++30%' END pct_gap                   
        ,group_id
        ,total_order_in_group
        -- ,gap_ship_fee
        ,((group_fee - (IF(CAST(order_cate AS BIGINT) = 0,1000,500) * 2 * (total_order_in_group - 1))) - sum_single_fee) AS gap_stack_fee
        ,sum_single_fee
        ,group_fee

FROM raw 
)
SELECT 
         report_date
        ,pick_city_id
        ,CASE
             WHEN CAST(order_cate AS BIGINT) = 0 THEN 'order-delivery'
             ELSE 'order-spxi' END AS source 
        ,distance_gap_range
        ,pct_gap
        ,SUM(total_order_in_group) AS total_order_over_paid
        ,SUM(gap_stack_fee) AS over_paid_amount    

FROM summary

WHERE pick_city_id in (217,218)
AND gap_stack_fee > 0
GROUP BY 1,2,3,4,5
