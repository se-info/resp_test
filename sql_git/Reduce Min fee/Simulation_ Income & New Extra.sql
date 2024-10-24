WITH group_cal AS 
(SELECT 
         group_id 
        ,group_code
        ,ref_order_category
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
            ),LEAST(SUM(single_fee),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_13k5

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_cal) + (MAX(fee_2_cal)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_cal) + (MAX(fee_1_cal)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_fee),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_fee)*IF(group_category=0,1,0.7),LEAST(SUM(single_fee),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))/MAX(rank_order)  
            ELSE MAX(final_stack_fee) END AS group_fee_allocate_13k5                     
        ----
        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt1) + (MAX(fee_2_opt1)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt1) + (MAX(fee_1_opt1)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt1),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt1)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt1),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_opt1

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt1) + (MAX(fee_2_opt1)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt1) + (MAX(fee_1_opt1)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt1),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))))/MAX(rank_order) 
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt1)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt1),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))/MAX(rank_order) 
         /MAX(rank_order)  
            ELSE MAX(final_stack_fee) END AS group_fee_allocate_opt1
        ------
        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt2) + (MAX(fee_2_opt2)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt2) + (MAX(fee_1_opt2)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt2),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt2)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt2),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_opt2

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt2) + (MAX(fee_2_opt2)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt2) + (MAX(fee_1_opt2)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt2),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))))/MAX(rank_order) 
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt2)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt2),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))/MAX(rank_order)  
            ELSE MAX(final_stack_fee) END AS group_fee_allocate_opt2  
        ------
        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt3) + (MAX(fee_2_opt3)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt3) + (MAX(fee_1_opt3)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt3),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt3)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt3),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_opt3

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt3) + (MAX(fee_2_opt3)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt3) + (MAX(fee_1_opt3)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt3),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))))/MAX(rank_order) 
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt3)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt3),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))/MAX(rank_order)  
            ELSE MAX(final_stack_fee) END AS group_fee_allocate_opt3  
        ------
        ,CASE WHEN MAX(rank_order) = 2 THEN
        GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt4) + (MAX(fee_2_opt4)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt4) + (MAX(fee_1_opt4)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt4),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate))))
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt4)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt4),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))  
         ELSE MAX(final_stack_fee) END AS group_fee_opt4

        ,CASE WHEN MAX(rank_order) = 2 THEN
        (GREATEST(
            GREATEST(
                ROUND(MAX(fee_1_opt4) + (MAX(fee_2_opt4)/MAX(re_stack))*MAX(rate_a)),
                ROUND(MAX(fee_2_opt4) + (MAX(fee_1_opt4)/MAX(re_stack))*MAX(rate_b))
            ),LEAST(SUM(single_opt4),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))))/MAX(rank_order) 
         WHEN MAX(rank_order) > 2 THEN GREATEST(SUM(single_opt4)*IF(group_category=0,1,0.7),LEAST(SUM(single_opt4),MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))/MAX(rank_order)  
            ELSE MAX(final_stack_fee) END AS group_fee_allocate_opt4  

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
        ----
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                               ,12500)
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_opt1 + gi.extra_fee_allocate
                    WHEN driver_policy = 2 AND ROUND(delivery_distance/CAST(1000 AS DOUBLE),1) <= 3 THEN 12500
                    WHEN driver_policy = 2 AND ROUND(delivery_distance/CAST(1000 AS DOUBLE)) <= 4 THEN 13500
                    ELSE 15000 END) AS opt1                    
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                               ,12000)
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_opt2 + gi.extra_fee_allocate
                    WHEN driver_policy = 2 AND ROUND(delivery_distance/CAST(1000 AS DOUBLE)) <= 3 THEN 12000
                    WHEN driver_policy = 2 AND ROUND(delivery_distance/CAST(1000 AS DOUBLE)) <= 4 THEN 13500
                    ELSE 15000 END) AS opt2 

        ----
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 AND ROUND(delivery_distance/CAST(1000 AS DOUBLE)) <= 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                                                                                      ,12000)
                    WHEN dot.group_id = 0 AND driver_policy != 2 AND ROUND(delivery_distance/CAST(1000 AS DOUBLE)) > 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                                                                                      ,13500)
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_opt3 + gi.extra_fee_allocate

                    WHEN driver_policy = 2 AND ROUND(delivery_distance/CAST(1000 AS DOUBLE)) <= 2 THEN 12000
                    WHEN driver_policy = 2 AND ROUND(delivery_distance/CAST(1000 AS DOUBLE)) <= 4 THEN 13500
                    ELSE 15000 END) AS opt3
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 AND ROUND(delivery_distance/CAST(1000 AS DOUBLE)) <= 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                                                                                      ,12000)
                    WHEN dot.group_id = 0 AND driver_policy != 2 AND ROUND(delivery_distance/CAST(1000 AS DOUBLE)) > 2 THEN GREATEST(doet.unit_fee * doet.surge_rate* (dot.delivery_distance/CAST(1000 AS DOUBLE))
                                                                                                                                      ,13500)
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_opt4 + gi.extra_fee_allocate
                    ELSE 12500 END) AS opt4
        ,CASE 
             WHEN doet.shift_category = 1 THEN '5 hour shift'
             WHEN doet.shift_category = 2 THEN '8 hour shift'
             WHEN doet.shift_category = 3 THEN '10 hour shift'
             WHEN doet.shift_category = 4 THEN '3 hour shift'
             ELSE 'Non Hub' end as hub_type                    
        
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
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
)
,extra_ship AS 
(SELECT 
         date_
        ,uid
        ,hub_type_original
        ,total_order
        ,extra_ship AS extra_current
        ,CASE 
             WHEN hub_type_original = '10 hour shift' THEN 30
             WHEN hub_type_original = '8 hour shift' THEN 25
             ELSE 0 END AS order_commitment 

FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics
WHERE total_order > 0 
)
,summary AS 
(SELECT
         raw.report_date
        ,raw.shipper_id 
        ,raw.source
        -- ,raw.is_hub
        ,raw.city_name
        ,pr.shipper_tier
        ,raw.hub_type
        ,CASE WHEN raw.is_hub = 'hub' THEN COALESCE(es.extra_current,0) ELSE 0 END AS extra_current
        ,CASE WHEN raw.is_hub = 'hub' THEN COALESCE(es.order_commitment,0) ELSE 0 END AS order_commitment
        ,COALESCE(pr.driver_daily_bonus,0) AS daily_bonus 
        ,COALESCE(pr.driver_other_income,0) AS other_income

        ,CASE WHEN raw.is_hub = 'hub' AND COALESCE(es.extra_current,0) > 0  AND COUNT(DISTINCT raw.ref_order_code) < COALESCE(es.order_commitment,0)  
              THEN COALESCE(es.order_commitment*12500,0) - SUM(raw.opt1) ELSE 0 END AS extra_opt1 
        ,CASE WHEN raw.is_hub = 'hub' AND COALESCE(es.extra_current,0) > 0 AND COUNT(DISTINCT raw.ref_order_code) < COALESCE(es.order_commitment,0)  
              THEN COALESCE(es.order_commitment*12000,0) - SUM(raw.opt2) ELSE 0 END AS extra_opt2
        ,CASE WHEN raw.is_hub = 'hub' AND COALESCE(es.extra_current,0) > 0 AND COUNT(DISTINCT raw.ref_order_code) < COALESCE(es.order_commitment,0)  
              THEN COALESCE(es.order_commitment*12000,0) - SUM(raw.opt3) ELSE 0 END AS extra_opt3 
        ,CASE WHEN raw.is_hub = 'hub' AND COALESCE(es.extra_current,0) > 0 AND COUNT(DISTINCT raw.ref_order_code) < COALESCE(es.order_commitment,0)  
              THEN COALESCE(es.order_commitment*12500,0) - SUM(raw.opt4) ELSE 0 END AS extra_opt4 

        ,SUM(raw.shipping_current) AS total_cost_current
        ,SUM(raw.opt1) AS total_cost_opt1
        ,SUM(raw.opt2) AS total_cost_opt2
        ,SUM(raw.opt3) AS total_cost_opt3
        ,SUM(raw.opt4) AS total_cost_opt4

        ,COUNT(DISTINCT raw.ref_order_code) AS cnt_order
        ,COUNT(DISTINCT CASE WHEN raw.group_id > 0 THEN raw.ref_order_code ELSE NULL END) AS cnt_stack
        ,COUNT(DISTINCT CASE WHEN raw.is_hub = 'hub' THEN raw.ref_order_code ELSE NULL END) AS cnt_hub_order
        ,COUNT(DISTINCT CASE WHEN raw.is_hub != 'hub' THEN raw.ref_order_code ELSE NULL END) AS cnt_non_hub_order


FROM raw 

LEFT JOIN dev_vnfdbi_opsndrivers.phong_driver_performance_raw pr 
    on pr.shipper_id = raw.shipper_id
    and pr.report_date = raw.report_date

LEFT JOIN extra_ship es 
    on es.uid = (CASE WHEN raw.is_hub = 'hub' THEN raw.shipper_id END) 
    and es.date_ = (CASE WHEN raw.is_hub = 'hub' THEN raw.report_date END)
    and es.hub_type_original = (CASE WHEN raw.is_hub = 'hub' THEN raw.hub_type END)

WHERE 1 = 1 
AND foody_service_id = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,es.order_commitment,raw.is_hub,es.extra_current
)
SELECT
        --  report_date
         CASE WHEN city_name in ('HCM City','Ha Noi City','Da Nang City') THEN city_name ELSE 'Others' END AS city_name
        ,CASE 
             WHEN shipper_tier = 'Hub' AND cnt_hub_order = 0 THEN 'Level 1'
             ELSE shipper_tier END AS shipper_tier 
        -- ,source
        ,SUM(cnt_order)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS cnt_order
        ,SUM(cnt_stack)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS cnt_stack
        ,(SUM(total_cost_current + extra_current + daily_bonus + other_income)/CAST(COUNT(shipper_id) AS DOUBLE)) AS income_current
        
        ,(SUM(total_cost_opt1 + extra_opt1 + daily_bonus + other_income)/CAST(COUNT(shipper_id) AS DOUBLE)) AS income_opt1
        ,(SUM(total_cost_opt2 + extra_opt2 + daily_bonus + other_income)/CAST(COUNT(shipper_id) AS DOUBLE)) AS income_opt2
        ,(SUM(total_cost_opt3 + extra_opt3 + daily_bonus + other_income)/CAST(COUNT(shipper_id) AS DOUBLE)) AS income_opt3
        ,(SUM(total_cost_opt4 + extra_opt4 + daily_bonus + other_income)/CAST(COUNT(shipper_id) AS DOUBLE)) AS income_opt4
        ,COUNT(shipper_id) AS total_shipper
        

FROM summary 


WHERE report_date between date'2023-06-26' AND date'2023-07-02'
 
GROUP BY 1,2

