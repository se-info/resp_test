-- temp
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
       ,dotet.driver_policy
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
        ,dotet.unit_fee_single
        ,dotet.min_fee_single
        ,dotet.surge_rate_single
        ,GREATEST(
                13500,
                ROUND(dotet.unit_fee_single * (dot.delivery_distance/CAST(1000 AS DOUBLE)) *
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
                13500,dotet.unit_fee_single 
                      * 1
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                ) AS single_fee_est
        ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 1 THEN GREATEST(
                13500,dotet.unit_fee_single 
                      * 1 
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                )
            ELSE 0
            END AS fee_1_est
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 2 THEN GREATEST(
                13500,dotet.unit_fee_single 
                      * 1
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                )
            ELSE 0
            END AS fee_2_est 
                
        -- #15000        
       ,GREATEST(
                12500,dotet.unit_fee_single 
                      * 1
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                ) AS single_12k5
        ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 1 THEN GREATEST(
                12500,dotet.unit_fee_single 
                      * 1 
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                )
            ELSE 0
            END AS fee_1_12k5
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 2 THEN GREATEST(
                12500,dotet.unit_fee_single 
                      * 1
                      * (dot.delivery_distance/CAST(1000 AS DOUBLE)) 
                )
            ELSE 0
            END AS fee_2_12k5


FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day ) dot 

LEFT JOIN (SELECT 
                  order_id
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee_single
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee_single
        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate_single
        ,CAST(JSON_EXTRACT(dotet.order_data,'$.shipper_policy.type') AS DOUBLE) AS driver_policy   
    FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da dotet
    where date(dt) = current_date - interval '1' day
          ) dotet on dotet.order_id = dot.id  

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogi 
    on ogi.id = dot.group_id 
    and ogi.ref_order_category = dot.ref_order_category

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city
    on city.id = dot.pick_city_id and city.country_id = 86

WHERE 1 = 1 
AND dot.group_id > 0 
-- AND dot.ref_order_category = 0
AND dot.order_status = 400
AND (
    --  DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN date'2022-12-01' AND DATE'2022-12-31'
    --  OR 
    --  DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN date'2023-03-01' AND DATE'2023-03-31'
    --  OR 
     DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN date'2023-05-01' AND current_date  - interval '1' day
     )   
     
;
-- FINAL

WITH group_cal AS 
(SELECT 
         group_id 
        ,group_code
        ,ref_order_category
        ,MAX(final_stack_fee) AS current_group_fee
        ,MAX(final_stack_fee)/MAX(rank_order) AS group_fee_allocate_current 
        ,MAX(rank_order) AS total_order_in_group
        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_est) + (MAX(fee_2_est)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_est) + (MAX(fee_1_est)/MAX(re_stack))*MAX(rate_b))
            ),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1)) + ( MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) ) 
         WHEN MAX(rank_order) > 2 THEN SUM(single_reduce)*IF(group_category=0,1,0.7)  
         ELSE MAX(final_stack_fee) END AS group_fee_13k5

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_est) + (MAX(fee_2_est)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_est) + (MAX(fee_1_est)/MAX(re_stack))*MAX(rate_b))
            ),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1)) + ( MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) ))/MAX(rank_order) 
            WHEN MAX(rank_order) > 2 THEN (SUM(single_reduce)*IF(group_category=0,1,0.7))/MAX(rank_order)  
            ELSE MAX(final_stack_fee) END AS group_fee_allocate_13k5                    

        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_12k5) + (MAX(fee_2_12k5)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_12k5) + (MAX(fee_1_12k5)/MAX(re_stack))*MAX(rate_b))
            ),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1)) + ( MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) ) 
         WHEN MAX(rank_order) > 2 THEN SUM(single_12k5)*IF(group_category=0,1,0.7)  
         ELSE SUM(single_12k5) END AS group_fee_12k5

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_12k5) + (MAX(fee_2_12k5)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_12k5) + (MAX(fee_1_12k5)/MAX(re_stack))*MAX(rate_b))
            ),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1)) + ( MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) ))/MAX(rank_order) 
             WHEN MAX(rank_order) > 2 THEN (SUM(single_12k5)*IF(group_category=0,1,0.7))/MAX(rank_order) 
             ELSE SUM(single_12k5) END AS group_fee_allocate_12k5  
        ,GREATEST(
                ROUND(MAX(fee_1_12k5) + (MAX(fee_2_12k5)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_12k5) + (MAX(fee_1_12k5)/MAX(re_stack))*MAX(rate_b))
                ) AS min_group_12k5

        ,MAX(unit_fee)*MAX(surge_rate)*MAX(group_distance) AS group_total_shipping_fee                

FROM dev_vnfdbi_opsndrivers.group_order_info_raw 


GROUP BY 1,2,3,group_category
) 
,raw AS
(SELECT 
         dot.uid AS shipper_id 
        ,DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date 
        ,dot.ref_order_code
        ,dot.ref_order_id
        ,dot.group_id
        ,gi.group_code 
        ,CASE 
              WHEN dot.ref_order_category = 0 THEN 'order_delivery'
              WHEN dot.ref_order_category = 6 THEN 'order_shopee'
              ELSE 'order_spxi' END AS source
        ,CASE WHEN doet.driver_policy = 2 THEN 1 ELSE 0 END AS is_hub
        ,doet.dotet_total_shipping_fee AS current_single_fee
        ,COALESCE(doet.return_fee,0) AS return_fee
        ,doet.unit_fee
        ,doet.min_fee
        ,doet.surge_rate
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN doet.dotet_total_shipping_fee
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_current
                    ELSE 13500 END) AS shipping_current
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                               ,12500)
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_12k5
                    ELSE 12500 END) AS shipping_new                    

FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (SELECT 
                    order_id
                    ,CAST(json_extract(order_data,'$.shipper_policy.type') as bigint) as driver_policy 
                    ,CAST(json_extract(doet.order_data,'$.delivery.shipping_fee.total') as double) as dotet_total_shipping_fee
                    ,CAST(json_extract(doet.order_data,'$.shopee.shipping_fee_info.return_fee') as double) as return_fee
                    ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                    ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                    ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate                    

        FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da doet
        WHERE date(dt) = current_date - interval '1' day
        ) doet 
    on dot.id = doet.order_id

LEFT JOIN group_cal gi 
    on gi.group_id = dot.group_id
    and gi.ref_order_category = dot.ref_order_category

WHERE 1 = 1 
AND dot.order_status in (400)
AND dot.group_id != 44680136
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
)
,extra_ship AS 
(SELECT 
         date_
        ,uid
        ,sum(total_order) AS total_order
        ,sum(extra_ship) AS extra_ship
        ,sum(new_extra) AS new_extra
FROM     
(SELECT 
         date_
        ,uid
        ,hub_type_original
        ,total_order
        ,extra_ship
        ,12500*(extra_ship/13500) AS new_extra  
        

FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics
WHERE total_order > 0 
)
GROUP BY 1,2
)
,summary AS
(SELECT 
         raw.report_date 
        ,raw.shipper_id
        ,dp.city_name
        ,dp.driver_income
        ,dp.driver_other_income
        ,dp.driver_daily_bonus
        ,dp.shipper_tier
        ,COALESCE(es.extra_ship,0) AS extra_ship
        ,COALESCE(es.new_extra,0) AS new_extra
        ,COUNT(DISTINCT raw.ref_order_code) AS total_order
        ,COUNT(DISTINCT CASE WHEN raw.is_hub = 1 THEN raw.ref_order_code ELSE NULL END) AS hub_order 
        ,SUM(raw.shipping_current) AS ship_current
        ,SUM(CASE WHEN raw.is_hub = 1 THEN raw.shipping_current ELSE NULL END) AS hub_ship_current
        ,SUM(raw.shipping_new) AS ship_new
        ,SUM(CASE WHEN is_hub = 1 THEN raw.shipping_new ELSE NULL END) AS hub_ship_new
        ,SUM(CASE WHEN is_hub = 0 THEN raw.shipping_new ELSE NULL END) AS non_hub_ship_new



FROM raw 

LEFT JOIN dev_vnfdbi_opsndrivers.phong_driver_performance_raw dp 
    on dp.shipper_id = raw.shipper_id 
    and dp.report_date = raw.report_date

LEFT JOIN extra_ship es 
    on es.uid = raw.shipper_id 
    and es.date_ = raw.report_date

WHERE 1 = 1 

AND raw.report_date BETWEEN date'2023-06-01' and current_date - interval '1' day

GROUP BY 1,2,3,4,5,6,7,8,9
)
-- SELECT * FROM summary where ship_new is null
SELECT 
         'June-Mtd' AS period
        ,'opt-1' AS metrics 
        ,CASE WHEN city_name IN ('HCM City','Ha Noi City','Da Nang City') THEN city_name ELSE 'Others' END AS city_name
        ,CASE 
              WHEN shipper_tier = 'Hub' AND hub_order = 0 THEN 'Level 1'
              WHEN city_name not in ('HCM City','Ha Noi City') THEN 'Other'
              ELSE shipper_tier END AS shipper_tier 
        ,SUM(COALESCE(non_hub_ship_new,0) + (COALESCE(hub_ship_current,0) + COALESCE(extra_ship,0) ) + COALESCE(driver_daily_bonus,0) + COALESCE(driver_other_income,0))/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS ship_new
        ,COUNT(shipper_id)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS active_driver



FROM summary 

GROUP BY 1,2,3,4

UNION ALL 
SELECT 
         'June-Mtd' AS period
        ,'opt-2' AS metrics 
        ,CASE WHEN city_name IN ('HCM City','Ha Noi City','Da Nang City') THEN city_name ELSE 'Others' END AS city_name
        ,CASE 
              WHEN shipper_tier = 'Hub' AND hub_order = 0 THEN 'Level 1'
              WHEN city_name not in ('HCM City','Ha Noi City') THEN 'Other'
              ELSE shipper_tier END AS shipper_tier 
        ,SUM(COALESCE(non_hub_ship_new,0) + (COALESCE(hub_ship_new,0) + COALESCE(new_extra,0) ) + COALESCE(driver_daily_bonus,0) + COALESCE(driver_other_income,0))/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS ship_new
        ,COUNT(shipper_id)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS active_driver



FROM summary 

GROUP BY 1,2,3,4