WITH assignment as 
(SELECT 
        sa.order_id
       ,COALESCE(ogm.ref_order_id,dot.ref_order_id) AS ref_order_id 
       ,COALESCE(ogm.ref_order_code,dot.ref_order_code) AS order_code
       ,COALESCE(ogm.ref_order_category,sa.order_type) AS order_category
       ,sa.status
       ,sa.shipper_uid AS driver_id
       ,FROM_UNIXTIME(sa.create_time - 3600) AS assign_time
       ,CASE 
            WHEN sa.assign_type = 1 then '1. Single Assign'
            WHEN sa.assign_type in (2,4) then '2. Multi Assign'
            WHEN sa.assign_type = 3 then '3. Well-Stack Assign'
            WHEN sa.assign_type = 5 then '4. Free Pick'
            WHEN sa.assign_type = 6 then '5. Manual'
            WHEN sa.assign_type in (7,8) then '6. New Stack Assign'
            ELSE NULL END AS assign_type
       ,CASE 
            WHEN sa.order_type = 200 then 'Group'
            ELSE 'Single' END AS order_type              

FROM 
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live


        UNION
    
        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
) sa 


LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogm 
    on ogm.group_id = (CASE WHEN sa.order_type = 200 then sa.order_id else 0 end)

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = (CASE WHEN sa.order_type != 200 then sa.order_id else 0 end)


WHERE 1 = 1
AND DATE(FROM_UNIXTIME(sa.create_time - 3600)) BETWEEN current_date - interval '90' day AND current_date - interval '1' day
)
,metrics AS
(SELECT 
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
       ,ogi.distance/CAST(100000 AS DOUBLE) AS group_distance
       ,ogi.ship_fee/CAST(100 AS DOUBLE) AS final_stack_fee 
       ,ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) AS rank_order
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 1 THEN dot.delivery_cost/CAST(100 AS DOUBLE)
            ELSE 0
            END AS fee_1
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY dot.group_id ORDER BY dot.real_pick_time ASC) = 2 THEN dot.delivery_cost/CAST(100 AS DOUBLE)
            ELSE 0
            END AS fee_2             
    --    ,ogi.extra_data 
       ,MAX_BY(sa.status,sa.assign_time) AS status 
       ,MAX_BY(sa.order_type,sa.assign_time) AS order_type
       ,MAX_BY(sa.assign_type,sa.assign_time) AS assign_type

FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day ) dot 

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet
    on dotet.order_id = dot.id

LEFT JOIN assignment sa 
    on sa.ref_order_id = dot.ref_order_id 
    and sa.order_category = dot.ref_order_category 
    and sa.status in (3,4)

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogi 
    on ogi.id = dot.group_id 
    and ogi.ref_order_category = dot.ref_order_category

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city
    on city.id = dot.pick_city_id and city.country_id = 86

WHERE 1 = 1 
AND dot.group_id > 0 
AND dot.ref_order_category = 0
AND order_status = 400
-- AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN date'2023-02-01' and date'2023-02-28'
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) = current_date - interval '1' day
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,dot.real_pick_time,dot.delivery_cost
)
,final_metrics AS
(SELECT 
        m.group_code
       ,m.group_category
       ,m.city_name
       ,m.report_date
       ,ARRAY_AGG(m.ref_order_code) AS ref_order_code_ext
       ,COUNT(DISTINCT m.ref_order_code) AS total_order_in_group
       ,SUM(m.single_fee) AS total_before_stack_fee
       ,SUM(CASE WHEN m.rank_order = 1 then m.extra_fee ELSE NULL END) AS extra_fee
       ,SUM(CASE WHEN m.rank_order = 1 then m.re_stack ELSE NULL END) AS re_system 
       ,SUM(m.fee_1) AS fee_1
       ,SUM(m.fee_2) AS fee_2
       ,SUM(CASE WHEN m.rank_order = 1 THEN m.rate_a ELSE NULL END) AS rate_a
       ,SUM(CASE WHEN m.rank_order = 1 THEN m.rate_b ELSE NULL END) AS rate_b
       ,SUM(CASE WHEN m.rank_order = 1 then ROUND((m.unit_fee * m.group_distance * m.surge_rate),1) ELSE NULL END) AS total_shipping_fee
       ,SUM(CASE WHEN m.rank_order = 1 then min_fee ELSE NULL END) AS min_shipping_fee
       ,SUM(CASE WHEN m.rank_order = 1 then final_stack_fee ELSE NULL END) AS final_stack_fee


FROM metrics m
WHERE 1 = 1
-- AND m.group_code = 'D41101506546'
AND order_type != 'Group' 

GROUP BY 1,2,3,4
    )
,summary AS
(SELECT 
         group_code
        ,group_category
        ,city_name
        ,report_date
        ,ref_order_code_ext
        ,min_shipping_fee AS min_shipping_fee_system
        ,total_shipping_fee AS total_shipping_fee_system
        ,final_stack_fee AS final_stack_fee_system
        ,total_order_in_group
        -- ,fee_1 + (fee_2/re_system) * rate_a AS a_current
        -- ,fee_2 + (fee_1/re_system) * rate_b AS b_current
        ,GREATEST((fee_1 + (fee_2/re_system) * rate_a),(fee_2 + (fee_1/re_system) * rate_b)) AS min_fee_cal
        ,GREATEST(
                GREATEST((fee_1 + (fee_2/re_system) * rate_a),(fee_2 + (fee_1/re_system) * rate_b))
                ,
                total_shipping_fee
                ) +  (extra_fee * total_order_in_group)
                AS final_stack_fee_cal_current
        --opt1                
        ,GREATEST((fee_1 + (fee_2/re_system) * 0.5),(fee_2 + (fee_1/re_system) * 0.5)) AS min_fee_opt1
        ,GREATEST(
                GREATEST((fee_1 + (fee_2/re_system) * 0.5),(fee_2 + (fee_1/re_system) * 0.5))
                ,
                total_shipping_fee
                ) +  (extra_fee * total_order_in_group)
                AS final_stack_fee_cal_opt1                
        --opt2               
        ,GREATEST((fee_1 + (fee_2/re_system) * 0.4),(fee_2 + (fee_1/re_system) * 0.4)) AS min_fee_opt2
        ,GREATEST(
                GREATEST((fee_1 + (fee_2/re_system) * 0.4),(fee_2 + (fee_1/re_system) * 0.4))
                ,
                total_shipping_fee
                ) +  (extra_fee * total_order_in_group)
                AS final_stack_fee_cal_opt2
        --opt3              
        ,GREATEST((fee_1 + (fee_2/re_system) * 0.3),(fee_2 + (fee_1/re_system) * 0.3)) AS min_fee_opt3
        ,GREATEST(
                GREATEST((fee_1 + (fee_2/re_system) * 0.3),(fee_2 + (fee_1/re_system) * 0.3))
                ,
                total_shipping_fee
                ) +  (extra_fee * total_order_in_group)
                AS final_stack_fee_cal_opt3
        --opt4             
        ,GREATEST((fee_1 + (fee_2/re_system) * 0.2),(fee_2 + (fee_1/re_system) * 0.2)) AS min_fee_opt4
        ,GREATEST(
                GREATEST((fee_1 + (fee_2/re_system) * 0.2),(fee_2 + (fee_1/re_system) * 0.2))
                ,
                total_shipping_fee
                ) +  (extra_fee * total_order_in_group)
                AS final_stack_fee_cal_opt4
        --opt5             
        ,GREATEST((fee_1 + (fee_2/re_system) * 0.1),(fee_2 + (fee_1/re_system) * 0.1)) AS min_fee_opt5
        ,GREATEST(
                GREATEST((fee_1 + (fee_2/re_system) * 0.1),(fee_2 + (fee_1/re_system) * 0.1))
                ,
                total_shipping_fee
                ) +  (extra_fee * total_order_in_group)
                AS final_stack_fee_cal_opt5                                
        --opt6             
        ,GREATEST((fee_1 + (fee_2/re_system) * 0),(fee_2 + (fee_1/re_system) * 0)) AS min_fee_opt6
        ,GREATEST(
                GREATEST((fee_1 + (fee_2/re_system) * 0),(fee_2 + (fee_1/re_system) * 0))
                ,
                total_shipping_fee
                ) +  (extra_fee * total_order_in_group)
                AS final_stack_fee_cal_opt6

FROM final_metrics
)
SELECT 
         report_date
        ,city_name
        ,SUM(total_order_in_group) AS total_order_stack
        ,SUM(final_stack_fee_cal_current) AS final_stack_fee_cal_current
        ,SUM(final_stack_fee_cal_opt1)/SUM(final_stack_fee_cal_current) -1 AS final_stack_fee_cal_opt1
        ,SUM(final_stack_fee_cal_opt2)/SUM(final_stack_fee_cal_current) -1 AS final_stack_fee_cal_opt2
        ,SUM(final_stack_fee_cal_opt3)/SUM(final_stack_fee_cal_current) -1 AS final_stack_fee_cal_opt3
        ,SUM(final_stack_fee_cal_opt4)/SUM(final_stack_fee_cal_current) -1 AS final_stack_fee_cal_opt4
        ,SUM(final_stack_fee_cal_opt5)/SUM(final_stack_fee_cal_current) -1 AS final_stack_fee_cal_opt5
        ,SUM(final_stack_fee_cal_opt6)/SUM(final_stack_fee_cal_current) -1 AS final_stack_fee_cal_opt6 

FROM summary

WHERE city_name = 'HCM City'
GROUP BY 1,2