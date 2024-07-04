-- CPO Estimation 
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

        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_12k) + (MAX(fee_2_12k)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_12k) + (MAX(fee_1_12k)/MAX(re_stack))*MAX(rate_b))
            ),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1)) + ( MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) ) 
         WHEN MAX(rank_order) > 2 THEN SUM(single_12k)*IF(group_category=0,1,0.7)  
         ELSE SUM(single_12k) END AS group_fee_12k

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_12k) + (MAX(fee_2_12k)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_12k) + (MAX(fee_1_12k)/MAX(re_stack))*MAX(rate_b))
            ),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1)) + ( MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) ))/MAX(rank_order) 
             WHEN MAX(rank_order) > 2 THEN (SUM(single_12k)*IF(group_category=0,1,0.7))/MAX(rank_order) 
             ELSE SUM(single_12k) END AS group_fee_allocate_12k 

FROM dev_vnfdbi_opsndrivers.group_order_info_raw 


GROUP BY 1,2,3,group_category
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
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                               ,12500)
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_12k5
                    ELSE 12500 END) AS shipping_12k5                    
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                               ,12000)
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_12k
                    ELSE 12000 END) AS shipping_12k        
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
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
)
,extra_ship AS 
(SELECT 
         date_
        ,uid
        ,sum(total_order) AS total_order
        ,sum(extra_ship) AS extra_ship
        ,sum(extra_12k5) AS extra_12k5
        ,sum(extra_12k) AS extra_12k

FROM     
(SELECT 
         date_
        ,uid
        ,hub_type_original
        ,total_order
        ,extra_ship
        ,12500*(extra_ship/13500) AS extra_12k5
        ,12000*(extra_ship/13500) AS extra_12k                
        

FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics
WHERE total_order > 0 
)
GROUP BY 1,2
)
,summary AS 
(SELECT
         raw.report_date
        ,raw.shipper_id 
        ,raw.source
        ,raw.is_hub
        ,raw.city_name
        ,COALESCE(es.extra_ship,0) AS extra_current
        ,COALESCE(es.extra_12k5,0) AS extra_12k5
        ,COALESCE(es.extra_12k,0) AS extra_12k
        ,COUNT(DISTINCT raw.ref_order_code) AS cnt_order
        ,COUNT(DISTINCT CASE WHEN raw.group_id > 0 THEN raw.ref_order_code ELSE NULL END) AS cnt_stack
        ,SUM(raw.shipping_current) AS total_cost_current
        ,SUM(raw.shipping_12k5) AS total_cost_12k5
        ,SUM(raw.shipping_12k) AS total_cost_12k


FROM raw 

LEFT JOIN extra_ship es 
    on es.uid = raw.shipper_id 
    and es.date_ = raw.report_date
    
WHERE 1 = 1 
-- AND report_date = date'2023-06-24'
-- AND source = 'order_delivery'
AND foody_service_id = 1
GROUP BY 1,2,3,4,5,6,7,8
)
SELECT 
        --  report_date
         CASE WHEN city_name in ('HCM City','Ha Noi City','Da Nang City') THEN city_name ELSE 'Others' END AS city_name
        ,is_hub 
        -- ,source
        ,SUM(cnt_order)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS cnt_order
        ,SUM(cnt_stack)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS cnt_stack
        ,(SUM(total_cost_current + extra_current)/CAST(COUNT(DISTINCT report_date) AS DOUBLE))/CAST(ex.exchange_rate AS DOUBLE) AS total_cost_current
        ,(SUM(total_cost_12k5 + extra_12k5)/CAST(COUNT(DISTINCT report_date) AS DOUBLE))/CAST(ex.exchange_rate AS DOUBLE) AS total_cost_12k5
        ,(SUM(total_cost_12k + extra_12k)/CAST(COUNT(DISTINCT report_date) AS DOUBLE))/CAST(ex.exchange_rate AS DOUBLE) AS total_cost_12k
        ,(SUM(extra_12k5)/CAST(COUNT(DISTINCT report_date) AS DOUBLE))/CAST(ex.exchange_rate AS DOUBLE) AS extra_12k5
        ,(SUM(extra_12k)/CAST(COUNT(DISTINCT report_date) AS DOUBLE))/CAST(ex.exchange_rate AS DOUBLE) AS extra_12k
        ,(SUM(extra_current)/CAST(COUNT(DISTINCT report_date) AS DOUBLE))/CAST(ex.exchange_rate AS DOUBLE) AS extra_current

FROM summary 

CROSS JOIN 
(SELECT 
       SUM(exchange_rate)/CAST(COUNT(DISTINCT grass_date) AS DOUBLE) AS exchange_rate
     
FROM mp_order.dim_exchange_rate__reg_s0_live
WHERE grass_date between date'2023-06-26' AND date'2023-07-02'
AND currency = 'VND'
) ex



WHERE report_date between date'2023-06-26' AND date'2023-07-02'
 
GROUP BY 1,2,ex.exchange_rate
;
-- Income Estimation
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

        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_12k) + (MAX(fee_2_12k)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_12k) + (MAX(fee_1_12k)/MAX(re_stack))*MAX(rate_b))
            ),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1)) + ( MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) ) 
         WHEN MAX(rank_order) > 2 THEN SUM(single_12k)*IF(group_category=0,1,0.7)  
         ELSE SUM(single_12k) END AS group_fee_12k

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_12k) + (MAX(fee_2_12k)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_12k) + (MAX(fee_1_12k)/MAX(re_stack))*MAX(rate_b))
            ),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*1)) + ( MAX(extra_fee) * 2 * (MAX(rank_order)  - 1) ))/MAX(rank_order) 
             WHEN MAX(rank_order) > 2 THEN (SUM(single_12k)*IF(group_category=0,1,0.7))/MAX(rank_order) 
             ELSE SUM(single_12k) END AS group_fee_allocate_12k 

FROM dev_vnfdbi_opsndrivers.group_order_info_raw 


GROUP BY 1,2,3,group_category
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
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                               ,12500)
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_12k5
                    ELSE 12500 END) AS shipping_12k5                    
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                               ,12000)
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_12k
                    ELSE 12000 END) AS shipping_12k        
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
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
)
,extra_ship AS 
(SELECT 
         date_
        ,uid
        ,sum(total_order) AS total_order
        ,sum(extra_ship) AS extra_ship
        ,sum(extra_12k5) AS extra_12k5
        ,sum(extra_12k) AS extra_12k

FROM     
(SELECT 
         date_
        ,uid
        ,hub_type_original
        ,total_order
        ,extra_ship
        ,12500*(extra_ship/13500) AS extra_12k5
        ,12000*(extra_ship/13500) AS extra_12k                
        

FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics
WHERE total_order > 0 
)
GROUP BY 1,2
)
,summary AS 
(SELECT
         raw.report_date
        ,raw.shipper_id 
        ,raw.source
        ,raw.is_hub
        ,raw.city_name
        ,COALESCE(es.extra_ship,0) AS extra_current
        ,COALESCE(es.extra_12k5,0) AS extra_12k5
        ,COALESCE(es.extra_12k,0) AS extra_12k
        ,COALESCE(pr.driver_daily_bonus,0) AS daily_bonus 
        ,COALESCE(pr.driver_other_income,0) AS other_income
        ,COUNT(DISTINCT raw.ref_order_code) AS cnt_order
        ,COUNT(DISTINCT CASE WHEN raw.group_id > 0 THEN raw.ref_order_code ELSE NULL END) AS cnt_stack
        ,SUM(raw.shipping_current) AS total_cost_current
        ,SUM(raw.shipping_12k5) AS total_cost_12k5
        ,SUM(raw.shipping_12k) AS total_cost_12k


FROM raw 

LEFT JOIN dev_vnfdbi_opsndrivers.phong_driver_performance_raw pr 
    on pr.shipper_id = raw.shipper_id
    and pr.report_date = raw.report_date

LEFT JOIN extra_ship es 
    on es.uid = raw.shipper_id 
    and es.date_ = raw.report_date
    
WHERE 1 = 1 
AND foody_service_id = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10
)
SELECT 
         CASE WHEN city_name in ('HCM City','Ha Noi City','Da Nang City') THEN city_name ELSE 'Others' END AS city_name
        ,is_hub 
        ,SUM(cnt_order)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS cnt_order
        ,SUM(cnt_stack)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS cnt_stack
        ,SUM(total_cost_current + extra_current + daily_bonus + other_income)/COUNT(shipper_id) AS income_current
        ,SUM(total_cost_12k5 + extra_12k5 + daily_bonus + other_income)/COUNT(shipper_id) AS income_12k5
        ,SUM(total_cost_12k + extra_12k + daily_bonus + other_income)/COUNT(shipper_id) AS income_12k
        ,COUNT(shipper_id) AS total_shipper

FROM summary s 

WHERE 1 = 1 
AND report_date BETWEEN date'2023-06-26' and date'2023-07-02'
-- AND report_date = date'2023-07-02'


GROUP BY 1,2 
