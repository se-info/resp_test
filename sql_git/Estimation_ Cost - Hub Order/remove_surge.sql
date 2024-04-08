-- Group temp
DROP TABLE IF EXISTS dev_vnfdbi_opsndrivers.group_order_info_raw;
CREATE TABLE IF NOT EXISTS dev_vnfdbi_opsndrivers.group_order_info_raw 
AS
SELECT 
        dot.ref_order_id
       ,dot.ref_order_code
       ,dot.ref_order_category
       ,dot.group_id
       ,ogi.group_code
       ,ogi.ref_order_category AS group_category
       ,city.name_en AS city_name
       ,DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date
    --    ,dotet.order_data
       ,CAST(JSON_EXTRACT(dotet.order_data,'$.shipper_policy.type') AS DOUBLE) AS driver_policy
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_a') AS DOUBLE) AS rate_a 
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee_details.mod5_rate_b') AS DOUBLE) AS rate_b
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.unit_fee') AS DOUBLE) AS unit_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.surge_rate') AS DOUBLE) AS surge_rate
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.min_fee') AS DOUBLE) AS min_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.ship_fee_info.extra_pickdrop_fee') AS DOUBLE) AS extra_fee
       ,CAST(JSON_EXTRACT(ogi.extra_data,'$.re') AS DOUBLE) AS re_stack       
       ,dot.delivery_cost/CAST(100 AS DOUBLE) AS single_fee
       ,dot.delivery_distance/CAST(1000 AS DOUBLE) AS single_distance   
       ,ogi.distance/CAST(100000 AS DOUBLE) AS group_distance
       ,ogi.ship_fee/CAST(100 AS DOUBLE) AS final_stack_fee 
       ,ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) AS rank_order
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee_single
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee_single
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate_single
        ,GREATEST(
                13500,
                ROUND(CAST(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') AS DOUBLE) * (dot.delivery_distance/CAST(1000 AS DOUBLE)) *
                      1
                     )
        ) AS single_reduce
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 1 THEN dot.delivery_cost/CAST(100 AS DOUBLE)
            ELSE 0
            END AS fee_1
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 2 THEN dot.delivery_cost/CAST(100 AS DOUBLE)
            ELSE 0
            END AS fee_2             
    --    ,ogi.extra_data 
       ,GREATEST(
                13500,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) 
                      * 1
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                ) AS single_fee_est
        ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 1 THEN GREATEST(
                13500,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) 
                      * 1 
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                )
            ELSE 0
            END AS fee_1_est
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 2 THEN GREATEST(
                13500,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) 
                      * 1
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                )
            ELSE 0
            END AS fee_2_est 
                
        -- #15000        
       ,GREATEST(
                15000,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) 
                      * 1
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                ) AS single_15k
        ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 1 THEN GREATEST(
                15000,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) 
                      * 1 
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                )
            ELSE 0
            END AS fee_1_15k
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 2 THEN GREATEST(
                15000,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) 
                      * 1
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                )
            ELSE 0
            END AS fee_2_15k


FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day ) dot 

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet
    on dotet.order_id = dot.id  

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogi 
    on ogi.id = dot.group_id 
    and ogi.ref_order_category = dot.ref_order_category

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city
    on city.id = dot.pick_city_id and city.country_id = 86

WHERE 1 = 1 
AND dot.group_id > 0 
AND dot.ref_order_category = 0
AND dot.order_status = 400
AND (
    --  DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN date'2022-12-01' AND DATE'2022-12-31'
    --  OR 
    --  DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN date'2023-03-01' AND DATE'2023-03-31'
    --  OR 
     DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN date'2023-05-01' AND current_date  - interval '1' day
     )   

;
-- Estimation
WITH group_cal AS 
(SELECT 
         group_id 
        ,group_code
        ,MAX(final_stack_fee) AS current_group_fee
        ,MAX(final_stack_fee)/MAX(rank_order) AS group_fee_allocate_current 
        ,MAX(rank_order) AS total_order_in_group
        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND((MAX(fee_1_est) + (MAX(fee_2_est)/MAX(re_stack)))*MAX(rate_a)),
                ROUND((MAX(fee_2_est) + (MAX(fee_1_est)/MAX(re_stack)))*MAX(rate_b))
            ),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1)) + ( MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) ) 
         ELSE SUM(single_reduce)*IF(group_category=0,1,0.7)  END AS reduce_group_fee

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND((MAX(fee_1_est) + (MAX(fee_2_est)/MAX(re_stack)))*MAX(rate_a)),
                ROUND((MAX(fee_2_est) + (MAX(fee_1_est)/MAX(re_stack)))*MAX(rate_b))
            ),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1)) + ( MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) ))/MAX(rank_order) 
            ELSE (SUM(single_reduce)*IF(group_category=0,1,0.7))/MAX(rank_order) END AS group_fee_allocate_reduce                            

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND((MAX(fee_1_15k) + (MAX(fee_2_15k)/MAX(re_stack)))*MAX(rate_a)),
                ROUND((MAX(fee_2_15k) + (MAX(fee_1_15k)/MAX(re_stack)))*MAX(rate_b))
            ),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1)) + ( MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) ) 
            )/MAX(rank_order) 
            ELSE (SUM(single_15k)*IF(group_category=0,1,0.7))/MAX(rank_order) END AS group_fee_allocate_15k

        ,(MAX(fee_1_est) + (MAX(fee_2_est)/MAX(re_stack)))*MAX(rate_a) AS a 
        ,(MAX(fee_2_est) + (MAX(fee_1_est)/MAX(re_stack)))*MAX(rate_b) AS b 
        ,ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1) AS total_shipping_fee
        ,MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) AS extra_fee
        ,SUM(single_reduce)*IF(group_category=0,1,0.7) AS group_fee

FROM dev_vnfdbi_opsndrivers.group_order_info_raw 

WHERE city_name = 'Ha Noi City'

GROUP BY 1,2,group_category
) 
,raw AS
(SELECT 
        raw.id as order_id
       ,dot.group_id 
       ,raw.created_date 
       ,HOUR(raw.created_timestamp)*100 + MINUTE(raw.created_timestamp) AS hour_minute
       ,FROM_UNIXTIME(dot.estimated_drop_time- 3600) AS estimation_time 
       ,CASE 
             WHEN (HOUR(raw.created_timestamp)*100 + MINUTE(raw.created_timestamp)) BETWEEN 1700 AND 1729 THEN '1. 17:00 - 17:29'
             WHEN (HOUR(raw.created_timestamp)*100 + MINUTE(raw.created_timestamp)) BETWEEN 1730 AND 1759 THEN '2. 17:30 - 17:59'
             WHEN (HOUR(raw.created_timestamp)*100 + MINUTE(raw.created_timestamp)) BETWEEN 1800 AND 1829 THEN '3. 18:00 - 18:29'
             WHEN (HOUR(raw.created_timestamp)*100 + MINUTE(raw.created_timestamp)) BETWEEN 1830 AND 1859 THEN '4. 18:30 - 18:59'
             WHEN (HOUR(raw.created_timestamp)*100 + MINUTE(raw.created_timestamp)) BETWEEN 1900 AND 1929 THEN '5. 19:00 - 19:29'
             WHEN (HOUR(raw.created_timestamp)*100 + MINUTE(raw.created_timestamp)) BETWEEN 1930 AND 1959 THEN '6. 19:30 - 19:59'
             WHEN (HOUR(raw.created_timestamp)*100 + MINUTE(raw.created_timestamp)) BETWEEN 2000 AND 2029 THEN '7. 20:00 - 20:29'
             WHEN (HOUR(raw.created_timestamp)*100 + MINUTE(raw.created_timestamp)) BETWEEN 2030 AND 2059 THEN '8. 20:30 - 20:59'
             WHEN (HOUR(raw.created_timestamp)*100 + MINUTE(raw.created_timestamp)) BETWEEN 2100 AND 2129 THEN '9. 21:00 - 21:29'
             WHEN (HOUR(raw.created_timestamp)*100 + MINUTE(raw.created_timestamp)) BETWEEN 2130 AND 2159 THEN '10. 21:30 - 21:59'
             ELSE '11. Normal Hour' END AS hour_range
       ,raw.city_name
       ,raw.district_id
       ,di.name_en AS district_name
       ,CAST(json_extract(doet.order_data,'$.shipper_policy.type') AS BIGINT) AS driver_policy  
       ,CAST(json_extract(doet.order_data,'$.delivery.shipping_fee.total') AS DOUBLE) as dotet_total_shipping_fee
       ,CAST(json_extract(doet.order_data,'$.shopee.shipping_fee_info.return_fee') AS DOUBLE) as return_fee
       ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.unit_fee') AS DOUBLE) as unit_fee
       ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.min_fee') AS DOUBLE) as min_fee
       ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.surge_rate') AS DOUBLE) as surge_rate   
       ,raw.order_status
       ,raw.cancel_reason
       ,raw.shipper_id
       ,CASE WHEN dot.group_id > 0
             THEN gc.group_fee_allocate_current
        ELSE             
       GREATEST(
                CAST(json_extract(doet.order_data,'$.shipping_fee_config.min_fee') AS DOUBLE),
                ROUND(CAST(json_extract(doet.order_data,'$.shipping_fee_config.unit_fee') AS DOUBLE) * distance *
                      CAST(json_extract(doet.order_data,'$.shipping_fee_config.surge_rate') AS DOUBLE)
                     )
        ) END AS shipping_cal

       ,CASE WHEN dot.group_id > 0
             THEN gc.group_fee_allocate_reduce
       ELSE
       GREATEST(
                13500,
                ROUND(CAST(json_extract(doet.order_data,'$.shipping_fee_config.unit_fee') AS DOUBLE) * distance *
                      1
                     )
        ) END AS shipping_reduce_surge                            
        
        ,CASE WHEN dot.group_id > 0
             THEN gc.group_fee_allocate_15k
       ELSE
       GREATEST(
                15000,
                ROUND(CAST(json_extract(doet.order_data,'$.shipping_fee_config.unit_fee') AS DOUBLE) * distance *
                      1
                     )
        ) END AS shipping_reduce_15k

FROM dev_vnfdbi_opsndrivers.phong_raw_order_v2 raw


LEFT JOIN shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di 
    on di.id = raw.district_id
    and di.province_id = raw.city_id

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE date(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = raw.id 
    and dot.ref_order_category = raw.order_type

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da WHERE date(dt) = current_date - interval '1' day) doet 
    on doet.order_id = dot.id

LEFT JOIN group_cal gc 
    on gc.group_id = dot.group_id

WHERE raw.created_date BETWEEN DATE'2023-05-14' AND DATE'2023-06-11'
AND city_id = 218
AND raw.order_type = 0
)
SELECT
        created_date
       ,hour_range
       ,SUM(gap) AS saving_v1
       ,SUM(gap_v2) AS saving_v2 
       ,COUNT(DISTINCT CASE WHEN gap > 0 THEN order_id ELSE NULL END) AS order_impacted 
       ,COUNT(DISTINCT order_id ) AS total_order
       ,COUNT(DISTINCT CASE WHEN driver_policy = 2 THEN order_id ELSE NULL END) AS hub_order
       ,COUNT(DISTINCT CASE WHEN driver_policy != 2 THEN order_id ELSE NULL END) AS non_hub_order


FROM
(SELECT  
        *
        ,CASE WHEN (surge_rate > 1 or min_fee > 13500) AND driver_policy != 2 THEN shipping_cal - shipping_reduce_surge ELSE 0 END AS gap 
         ,CASE WHEN (surge_rate > 1 or min_fee > 13500) AND driver_policy != 2 THEN shipping_cal - shipping_reduce_15k ELSE 0 END AS gap_v2 

FROM raw
WHERE 1 = 1 
AND DATE_FORMAT(created_date,'%a') = 'Sun'
AND order_status = 'Delivered'
)

GROUP BY 1,2
--         created_date
--        ,city_name
--        ,CASE 
--             WHEN district_name in 
--                                 ('Dong Da',
--                                 'Hai Ba Trung',
--                                 'Hoan Kiem',
--                                 'Ba Dinh',
--                                 'Cau Giay',
--                                 'Ha Dong',
--                                 'Thanh Xuan') THEN district_name ELSE 'Other' END AS district_name
--        ,hour_range
--        ,COUNT(DISTINCT order_id)/CAST(COUNT(DISTINCT created_date) AS DOUBLE) AS gross_order
--        ,COUNT(DISTINCT CASE WHEN order_status = 'Delivered' THEN order_id ELSE NULL END) AS net_order 
--        ,COUNT(DISTINCT CASE WHEN order_status = 'Delivered' AND driver_policy != 2 THEN order_id ELSE NULL END) AS net_order_non_hub 
--        ,COUNT(DISTINCT CASE WHEN order_status = 'Delivered' AND driver_policy = 2 THEN order_id ELSE NULL END) AS net_order_hub 
--        ,COUNT(DISTINCT CASE WHEN order_status = 'Cancelled' AND cancel_reason = 'No driver' THEN order_id ELSE NULL END) AS cnd_order
--        ,COUNT(DISTINCT CASE WHEN order_status = 'Delivered'  AND driver_policy != 2 THEN shipper_id ELSE NULL END) AS active_non_hub
--        ,COUNT(DISTINCT CASE WHEN order_status = 'Delivered'  AND driver_policy = 2 THEN shipper_id ELSE NULL END) AS active_hub
--        ,COUNT(DISTINCT CASE WHEN order_status = 'Delivered' AND surge_rate > 1 THEN order_id ELSE NULL END) AS net_order_have_surge 
--        ,SUM(dotet_total_shipping_fee - shipping_reduce_surge) AS saving_vnd
       

-- FROM raw 
-- WHERE 1 = 1 
-- AND DATE_FORMAT(created_date,'%a') = 'Sun'
-- GROUP BY 1,2,3,4
