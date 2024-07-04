WITH group_cal AS 
(SELECT 
         group_id 
        ,group_code
        ,group_category
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
            ),LEAST(SUM(single_fee),ROUND(MAX(ROUND(group_distance,1))*MAX(unit_fee)*MAX(surge_rate)))))/MAX(rank_order) 
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

       ,CARDINALITY(array_agg(pick_latitude)) AS count_pick_lat
       ,CARDINALITY(array_agg(DISTINCT pick_latitude)) AS count_pick_lat_unique
       ,CARDINALITY(array_agg(pick_longitude)) AS count_pick_long
       ,CARDINALITY(array_agg(DISTINCT pick_longitude)) AS count_pick_long_unique

                    
       ,CARDINALITY(array_agg(drop_latitude)) AS count_drop_lat
       ,CARDINALITY(array_agg(DISTINCT drop_latitude)) AS count_drop_lat_unique
       ,CARDINALITY(array_agg(drop_longitude)) AS count_drop_long
       ,CARDINALITY(array_agg(DISTINCT drop_longitude)) AS count_drop_long_unique
       ,MAX(extra_fee) AS extra_fee_config
       ,(CASE 
                WHEN CARDINALITY(array_agg(DISTINCT pick_latitude)) = CARDINALITY(array_agg(DISTINCT pick_longitude)) THEN IF(group_category=0,1000,500) * (CARDINALITY(array_agg(DISTINCT pick_latitude)) - 1)
                WHEN CARDINALITY(array_agg(DISTINCT pick_latitude)) != CARDINALITY(array_agg(DISTINCT pick_longitude)) THEN IF(group_category=0,1000,500) *(GREATEST(CARDINALITY(array_agg(DISTINCT pick_latitude)),CARDINALITY(array_agg(DISTINCT pick_longitude))) -1)
                END) +
            (CASE 
                WHEN CARDINALITY(array_agg(DISTINCT drop_latitude)) = CARDINALITY(array_agg(DISTINCT drop_longitude)) THEN IF(group_category=0,1000,500) * (CARDINALITY(array_agg(DISTINCT drop_latitude)) - 1)
                WHEN CARDINALITY(array_agg(DISTINCT drop_latitude)) != CARDINALITY(array_agg(DISTINCT drop_longitude)) THEN IF(group_category=0,1000,500) * (GREATEST(CARDINALITY(array_agg(DISTINCT drop_latitude)),CARDINALITY(array_agg(DISTINCT drop_longitude))) -1)
                END) AS extra_fee_cal
        ,((CASE 
                WHEN CARDINALITY(array_agg(DISTINCT pick_latitude)) = CARDINALITY(array_agg(DISTINCT pick_longitude)) THEN IF(group_category=0,1000,500) * (CARDINALITY(array_agg(DISTINCT pick_latitude)) - 1)
                WHEN CARDINALITY(array_agg(DISTINCT pick_latitude)) != CARDINALITY(array_agg(DISTINCT pick_longitude)) THEN IF(group_category=0,1000,500) *(GREATEST(CARDINALITY(array_agg(DISTINCT pick_latitude)),CARDINALITY(array_agg(DISTINCT pick_longitude))) -1)
                END) +
            (CASE 
                WHEN CARDINALITY(array_agg(DISTINCT drop_latitude)) = CARDINALITY(array_agg(DISTINCT drop_longitude)) THEN IF(group_category=0,1000,500) * (CARDINALITY(array_agg(DISTINCT drop_latitude)) - 1)
                WHEN CARDINALITY(array_agg(DISTINCT drop_latitude)) != CARDINALITY(array_agg(DISTINCT drop_longitude)) THEN IF(group_category=0,1000,500) * (GREATEST(CARDINALITY(array_agg(DISTINCT drop_latitude)),CARDINALITY(array_agg(DISTINCT drop_longitude))) -1)
                END))*0.5 AS extra_fee_cal_allocate

-- #500d extra
       ,(CASE 
                WHEN CARDINALITY(array_agg(DISTINCT pick_latitude)) = CARDINALITY(array_agg(DISTINCT pick_longitude)) THEN IF(group_category=0,1000,500)*1.0/2 * (CARDINALITY(array_agg(DISTINCT pick_latitude)) - 1)
                WHEN CARDINALITY(array_agg(DISTINCT pick_latitude)) != CARDINALITY(array_agg(DISTINCT pick_longitude)) THEN IF(group_category=0,1000,500)*1.0/2 *(GREATEST(CARDINALITY(array_agg(DISTINCT pick_latitude)),CARDINALITY(array_agg(DISTINCT pick_longitude))) -1)
                END) +
            (CASE 
                WHEN CARDINALITY(array_agg(DISTINCT drop_latitude)) = CARDINALITY(array_agg(DISTINCT drop_longitude)) THEN IF(group_category=0,1000,500)*1.0/2 * (CARDINALITY(array_agg(DISTINCT drop_latitude)) - 1)
                WHEN CARDINALITY(array_agg(DISTINCT drop_latitude)) != CARDINALITY(array_agg(DISTINCT drop_longitude)) THEN IF(group_category=0,1000,500)*1.0/2 * (GREATEST(CARDINALITY(array_agg(DISTINCT drop_latitude)),CARDINALITY(array_agg(DISTINCT drop_longitude))) -1)
                END) AS extra_fee_cal_opt1
        ,((CASE 
                WHEN CARDINALITY(array_agg(DISTINCT pick_latitude)) = CARDINALITY(array_agg(DISTINCT pick_longitude)) THEN IF(group_category=0,1000,500)*1.0/2 * (CARDINALITY(array_agg(DISTINCT pick_latitude)) - 1)
                WHEN CARDINALITY(array_agg(DISTINCT pick_latitude)) != CARDINALITY(array_agg(DISTINCT pick_longitude)) THEN IF(group_category=0,1000,500)*1.0/2 *(GREATEST(CARDINALITY(array_agg(DISTINCT pick_latitude)),CARDINALITY(array_agg(DISTINCT pick_longitude))) -1)
                END) +
            (CASE 
                WHEN CARDINALITY(array_agg(DISTINCT drop_latitude)) = CARDINALITY(array_agg(DISTINCT drop_longitude)) THEN IF(group_category=0,1000,500)*1.0/2 * (CARDINALITY(array_agg(DISTINCT drop_latitude)) - 1)
                WHEN CARDINALITY(array_agg(DISTINCT drop_latitude)) != CARDINALITY(array_agg(DISTINCT drop_longitude)) THEN IF(group_category=0,1000,500)*1.0/2 * (GREATEST(CARDINALITY(array_agg(DISTINCT drop_latitude)),CARDINALITY(array_agg(DISTINCT drop_longitude))) -1)
                END))/MAX(rank_order) AS extra_fee_cal_allocate_opt1

FROM dev_vnfdbi_opsndrivers.group_order_info_raw 
-- WHERE group_id = 48243127
-- WHERE extra_fee is null
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
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN doet.dotet_total_shipping_fee
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_13k5 + (gi.extra_fee_cal_allocate_opt1)
                    WHEN driver_policy = 2 THEN 13500
                    ELSE 13500 END) AS opt1                           
                
        ,ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN doet.dotet_total_shipping_fee
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_13k5
                    WHEN driver_policy = 2 THEN 13500
                    ELSE 13500 END) AS opt2
        ,CASE 
        WHEN ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN doet.dotet_total_shipping_fee
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_current
                    ELSE 13500 END) > 
                    ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN doet.dotet_total_shipping_fee
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_13k5 + (gi.extra_fee_cal_allocate_opt1)
                    WHEN driver_policy = 2 THEN 13500
                    ELSE 13500 END) THEN 1 ELSE 0 END AS is_impact_opt1
        ,CASE 
        WHEN ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN doet.dotet_total_shipping_fee
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_current
                    ELSE 13500 END) > 
                    ROUND(CASE 
                    WHEN dot.group_id = 0 AND driver_policy != 2 THEN doet.dotet_total_shipping_fee
                    WHEN dot.group_id > 0 AND driver_policy != 2 THEN gi.group_fee_allocate_13k5
                    WHEN driver_policy = 2 THEN 13500
                    ELSE 13500 END) THEN 1 ELSE 0 END AS is_impact_opt2
        ,(gi.extra_fee_allocate*1.0/2) AS new_extra_fee
        ,gi.extra_fee_allocate AS current_extra_fee                    

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
    and gi.group_category = dot.ref_order_category

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city    
    on city.id = dot.pick_city_id
    and city.country_id = 86

WHERE 1 = 1 
AND dot.ref_order_category = 0
AND dot.ref_order_status = 7
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) between date'2023-08-14' and date'2023-08-20'
-- current_date - interval '30' day and current_date - interval '1' day
)
,metrics AS 
(SELECT
         raw.report_date
        ,raw.shipper_id 
        ,raw.source
        ,raw.is_hub
        ,raw.city_name
        ,COALESCE(dp.driver_other_income,0) driver_other_income
        ,COALESCE(dp.driver_daily_bonus,0) driver_daily_bonus
        
        ,COUNT(DISTINCT raw.ref_order_code) AS cnt_order
        ,COUNT(DISTINCT CASE WHEN raw.group_id > 0 THEN raw.ref_order_code ELSE NULL END) AS cnt_stack
        ,SUM(raw.shipping_current) AS total_cost_current
        ,SUM(raw.opt1) AS total_cost_opt1
        ,SUM(raw.opt2) AS total_cost_opt2
        ,SUM(CASE WHEN raw.group_id > 0 THEN raw.shipping_current ELSE NULL END) AS stack_cost_current
        ,SUM(CASE WHEN raw.group_id > 0 THEN raw.opt1 ELSE NULL END) AS stack_cost_opt1
        ,SUM(CASE WHEN raw.group_id > 0 THEN raw.opt2 ELSE NULL END) AS stack_cost_opt2



        ,COUNT(DISTINCT CASE WHEN raw.group_id > 0 AND raw.is_impact_opt1 = 1 THEN raw.ref_order_code ELSE NULL END) AS impact_opt1 
        ,COUNT(DISTINCT CASE WHEN raw.group_id > 0 AND raw.is_impact_opt2 = 1 THEN raw.ref_order_code ELSE NULL END) AS impact_opt2 


FROM raw 

LEFT JOIN dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab dp 
    on dp.report_date = raw.report_date
    and dp.shipper_id = raw.shipper_id
    
WHERE 1 = 1 
AND foody_service_id = 1
GROUP BY 1,2,3,4,5,6,7
)
SELECT 
        city_tier,
        SUM(income_current)*1.00/SUM(working_days) AS income_current,
        SUM(income_opt1)*1.00/SUM(working_days) AS income_opt1,
        SUM(income_opt2)*1.00/SUM(working_days) AS income_opt2,
        SUM(bonus_oth_income)*1.00/SUM(working_days) AS bonus_oth_income,
        SUM(working_days)*1.000/COUNT(DISTINCT shipper_id) AS avg_working_days

FROM
(SELECT
                       
                                                                                                                           
        CASE 
        WHEN city_name IN ('HCM City', 'Ha Noi City', 'Da Nang City') THEN city_name
        WHEN city_name IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') THEN 'T2'
        ELSE 'T3' END AS city_tier,
         is_hub,
         shipper_id,
         SUM(total_cost_current) AS income_current,
         SUM(total_cost_opt1) AS income_opt1,
         SUM(total_cost_opt2) AS income_opt2,
         (SUM(driver_daily_bonus) + SUM(driver_other_income)) AS bonus_oth_income,
         COUNT(DISTINCT report_date) AS working_days

        -- ,(SUM(total_cost_current)/CAST(COUNT(DISTINCT report_date) AS DOUBLE))/CAST(ex.exchange_rate AS DOUBLE) AS total_cost_current
        -- ,(SUM(total_cost_opt1)/CAST(COUNT(DISTINCT report_date) AS DOUBLE))/CAST(ex.exchange_rate AS DOUBLE) AS total_cost_opt1
        -- ,(SUM(total_cost_opt2)/CAST(COUNT(DISTINCT report_date) AS DOUBLE))/CAST(ex.exchange_rate AS DOUBLE) AS total_cost_opt2

FROM metrics 

CROSS JOIN 
(SELECT 
       SUM(exchange_rate)/CAST(COUNT(DISTINCT grass_date) AS DOUBLE) AS exchange_rate
     
FROM mp_order.dim_exchange_rate__reg_s0_live
WHERE grass_date between date'2023-08-14' and date'2023-08-20'
AND currency = 'VND'
) ex

WHERE 1 = 1 
AND is_hub != 'hub'

GROUP BY 1,2,3)
GROUP BY 1
;

-- select report_date,shipper_id,driver_income,driver_other_income,driver_daily_bonus from driver_ops_driver_performance_tab WHERE shipper_id = 41170557 AND report_date = date'2023-09-10'
-- and report_date between date'2023-08-14' and date'2023-08-20'
