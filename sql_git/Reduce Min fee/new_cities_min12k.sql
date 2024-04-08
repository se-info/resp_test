WITH group_cal AS 
(SELECT 
         group_id 
        ,group_code
        ,ref_order_category
        ,report_date
        ,MAX(driver_policy) AS driver_policy_group
        ,MAX(final_stack_fee) AS current_group_fee
        ,MAX(final_stack_fee)/MAX(rank_order) AS group_fee_allocate_current 
        ,MAX(rank_order) AS total_order_in_group
        ,ROUND(CASE WHEN MAX(rank_order) >= 2 THEN 
        MAX(final_stack_fee) - (CASE WHEN MAX(rank_order) = 2 THEN 
         ROUND(GREATEST(
                  MAX(min_fee),LEAST(SUM(single_fee),MAX(unit_fee)*MAX(group_distance)*MAX(surge_rate))
                  ))
              WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))   
              ELSE MAX(final_stack_fee) END) END) AS extra_fee
        
        ,ROUND(CASE WHEN MAX(rank_order) >= 2 THEN 
        MAX(final_stack_fee) - (CASE WHEN MAX(rank_order) = 2 THEN 
         ROUND(GREATEST(
                  MAX(min_fee),LEAST(SUM(single_fee),MAX(unit_fee)*MAX(group_distance)*MAX(surge_rate))
                  ))
              WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))   
              ELSE MAX(final_stack_fee) END) END)/MAX(rank_order) AS extra_fee_allocate

        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_cal) + (MAX(fee_2_cal)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_cal) + (MAX(fee_1_cal)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_cal),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))
            )
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_cal)*IF(group_category=0,1,0.7),LEAST(SUM(single_cal),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_cal

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_cal) + (MAX(fee_2_cal)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_cal) + (MAX(fee_1_cal)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_cal),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))))/MAX(rank_order)
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_cal)*IF(group_category=0,1,0.7),LEAST(SUM(single_cal),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))/MAX(rank_order)  
            ELSE MAX(final_stack_fee) END AS group_fee_allocate_cal

-- 12k
        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_12k) + (MAX(fee_2_12k)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_12k) + (MAX(fee_1_12k)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_fee_12k),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee_12k)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee_12k),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_12k

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_12k) + (MAX(fee_2_12k)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_12k) + (MAX(fee_1_12k)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_fee_12k),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))))/MAX(rank_order)
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee_12k)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee_12k),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))/MAX(rank_order)  
            ELSE MAX(final_stack_fee) END AS group_fee_allocate_12k

--
        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_12k_x_unit) + (MAX(fee_2_12k_x_unit)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_12k_x_unit) + (MAX(fee_1_12k_x_unit)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_fee_12k_x_unit),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee_12k_x_unit)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee_12k_x_unit),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_12k_x_unit

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_12k_x_unit) + (MAX(fee_2_12k_x_unit)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_12k_x_unit) + (MAX(fee_1_12k_x_unit)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_fee_12k_x_unit),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))))/MAX(rank_order)
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee_12k_x_unit)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee_12k_x_unit),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))/MAX(rank_order)  
            ELSE MAX(final_stack_fee) END AS group_fee_allocate_12k_x_unit

FROM dev_vnfdbi_opsndrivers.group_order_info_raw 
WHERE 1 = 1 
AND ref_order_category = 0
GROUP BY 1,2,3,4,group_category
)
,raw AS  
(SELECT 
         dot.uid AS shipper_id 
        ,DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date 
        ,dot.ref_order_code
        ,dot.ref_order_id
        ,dot.pick_city_id
        ,dot.group_id
        ,city.name_en AS city_name
        ,gi.group_code 
        ,CASE 
              WHEN dot.ref_order_category = 0 THEN 'order_delivery'
              WHEN dot.ref_order_category = 6 THEN 'order_shopee'
              ELSE 'order_spxi' END AS source
        ,CASE WHEN doet.driver_policy = 2 THEN 'hub' ELSE 'non-hub' END AS is_hub
        ,doet.dotet_total_shipping_fee AS current_single_fee
        ,COALESCE(doet.return_fee,0) AS return_fee
        ,doet.unit_fee
        ,doet.min_fee
        ,doet.surge_rate
        ,oct.foody_service_id
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN doet.dotet_total_shipping_fee
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_current
                    ELSE 13500 END) AS shipping_current
        ----
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                               ,12000)
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_12k + gi.extra_fee_allocate
                    WHEN driver_policy = 2 THEN 13500
                    ELSE (dot.delivery_cost*1.00)/100 END) AS shipping_fee_12k     

        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN GREATEST(3750 * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                               ,12000)
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_12k_x_unit + gi.extra_fee_allocate
                    WHEN driver_policy = 2 THEN 13500
                    ELSE (dot.delivery_cost*1.00)/100 END) AS shipping_fee_12k_x_unit        

        ,(dot.delivery_cost*1.00)/100 AS delivery_cost
        ,(dot.delivery_distance*1.00)/1000 AS distance
        
FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (SELECT id,foody_service_id FROM shopeefood.shopeefood_mart_dwd_vn_order_completed_da WHERE date(dt) = current_date - interval '1' day) oct 
    on oct.id = dot.ref_order_id
    and dot.ref_order_category = 0

LEFT JOIN (SELECT 
                    order_id
                    ,CAST(json_extract(order_data,'$.shipper_policy.type') as bigint) as driver_policy 
                    ,CAST(json_extract(doet.order_data,'$.delivery.shipping_fee.total') as double) as dotet_total_shipping_fee
                    ,CAST(json_extract(doet.order_data,'$.shopee.shipping_fee_info.return_fee') as double) as return_fee
                    ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                    ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                    ,CAST(json_extract(doet.order_data,'$.shipping_fee_config.surge_rate') as double) as surge_rate
                    ,CAST(json_extract_scalar(doet.order_data,'$.shipper_policy.shift_category') AS BIGINT) AS shift_category                    

        FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da doet
        WHERE date(dt) = current_date - interval '1' day
        ) doet 
    on dot.id = doet.order_id

LEFT JOIN group_cal gi 
    on gi.group_id = dot.group_id
    and gi.ref_order_category = dot.ref_order_category

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city    
    on city.id = dot.pick_city_id
    and city.country_id = 86

WHERE 1 = 1 
AND dot.ref_order_category = 0
AND dot.ref_order_status = 7
AND dot.order_status = 400
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) between DATE'2023-09-10' AND DATE'2023-09-18'
AND doet.driver_policy != 2 
AND REGEXP_LIKE(LOWER(city.name_en),'thanh hoa|dak lak|binh dinh|binh thuan') = true
)
SELECT 
        report_date,
        city_name,
        COUNT(DISTINCT ref_order_code) AS total_order,
        COUNT(DISTINCT CASE WHEN group_id > 0 THEN ref_order_code ELSE NULL END) AS group_order,
        SUM(shipping_current) AS total_shipping_fee_current,
        SUM(shipping_fee_12k) AS total_shipping_fee_12k,
        SUM(shipping_fee_12k_x_unit) AS total_shipping_fee_12k_x_unit,
        SUM(distance) AS total_distance

FROM raw 
WHERE foody_service_id = 1 
GROUP BY 1,2
-- thanh hoa, dak lak, binh dinh, binh thuan
