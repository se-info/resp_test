WITH assignment AS 
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
AND DATE(FROM_UNIXTIME(sa.create_time - 3600)) BETWEEN current_date - interval '60' day AND current_date - interval '1' day
)
,raw AS
(SELECT 
         ro.date_ AS report_date
        ,ro.partner_id AS shipper_id 
        ,ro.order_id 
        ,ro.source
        ,ro.city_name
        ,ro.delivered_by
        ,(driver_cost_base + return_fee_share_basic)/exchange_rate as dr_cost_base_usd
        ,(driver_cost_surge + return_fee_share_surge)/exchange_rate as dr_cost_surge_usd
        ,(case 
                when is_nan(bonus) = true then 0.00 
                when delivered_by = 'hub' then bonus_hub
                when delivered_by != 'hub' then bonus_non_hub
                else null end)  /exchange_rate as dr_cost_bonus_usd                
        ,MAX(sa.assign_time) AS last_assign_timestamp

FROM dev_vnfdbi_opsndrivers.shopeefood_vn_bnp_bill_fee_and_bonus_order_level ro 
LEFT JOIN assignment sa 
    ON sa.ref_order_id = ro.order_id
    and sa.order_category = 0


           
WHERE 1 = 1     
AND ro.source in ('Food','Market')
AND ro.status = 7
AND ro.date_ between current_date - interval '2' day and current_date - interval '1' day
GROUP BY 1,2,3,4,5,6,7,8,9
)
,metrics AS
(SELECT 
        raw.report_date
       ,raw.shipper_id 
       ,CAST(json_extract(hub.extra_data,'$.shift_category_name') AS VARCHAR) AS hub_type
       ,COALESCE(hub.slot_id,0) AS slot_id_hub
       ,delivered_by
       ,FROM_UNIXTIME(CAST(CAST(json_extract(extra_data,'$.shift_time_range') AS array(json))[1] AS bigint) - 3600) AS start_shift_time
       ,DATE_DIFF('second',
                    FROM_UNIXTIME(CAST(CAST(json_extract(extra_data,'$.shift_time_range') AS array(json))[1] AS bigint) - 3600)
                   ,FROM_UNIXTIME(CAST(CAST(json_extract(extra_data,'$.shift_time_range') AS array(json))[2] AS bigint) - 3600)) AS hour_shift                    
       ,ROW_NUMBER()OVER(PARTITION BY raw.shipper_id,raw.report_date 
                    order by FROM_UNIXTIME(CAST(CAST(json_extract(extra_data,'$.shift_time_range') AS array(json))[1] AS bigint) - 3600) ASC) AS rank_shift

       ,COUNT(DISTINCT (order_id,slot_id)) AS total_completed_order
       ,(SUM(dr_cost_base_usd)
        +SUM(dr_cost_surge_usd) 
        +SUM(dr_cost_bonus_usd) 
        ) AS driver_cost
       ,(SUM(dr_cost_base_usd)
        +SUM(dr_cost_surge_usd) 
        +SUM(dr_cost_bonus_usd) 
        ) / COUNT(DISTINCT (order_id,slot_id)) AS driver_cpo_base_surge_bonus
FROM raw

LEFT JOIN (select * from shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live ) hub 
    on hub.uid = raw.shipper_id 
       and DATE(from_unixtime(hub.report_date - 3600)) = raw.report_date
       and raw.last_assign_timestamp 
       between 
       FROM_UNIXTIME(CAST(CAST(json_extract(extra_data,'$.shift_time_range') AS array(json))[1] AS bigint) - 3600)
       and 
       FROM_UNIXTIME(CAST(CAST(json_extract(extra_data,'$.shift_time_range') AS array(json))[2] AS bigint) - 3600)


GROUP BY 1,2,3,4,5,6,7
)
,summary AS 
(SELECT 
        *
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY shipper_id,report_date ORDER BY start_shift_time ASC) != 1 THEN 'non-hub'
            ELSE delivered_by END AS case1 
       ,CASE 
            WHEN ROW_NUMBER()OVER(PARTITION BY shipper_id,report_date ORDER BY start_shift_time DESC) != 1 THEN 'non-hub' 
            ELSE delivered_by END AS case2
       ,CASE
            WHEN ROW_NUMBER()OVER(PARTITION BY shipper_id,report_date ORDER BY hour_shift DESC) != 1 THEN 'non-hub'
            ELSE delivered_by END AS case3

FROM metrics
WHERE 1 = 1
)
SELECT 
        report_date
       ,SUM(total_completed_order) AS ado
       ,SUM(CASE WHEN delivered_by = 'hub' then total_completed_order ELSE NULL END) AS ado_hub
       ,SUM(CASE WHEN delivered_by = 'non-hub' then total_completed_order ELSE NULL END) AS ado_non_hub
       ,SUM(CASE WHEN delivered_by = 'hub' then total_completed_order ELSE NULL END)/CAST(SUM(total_completed_order) AS DOUBLE) AS hub_coverage_current
       ,SUM(CASE WHEN case1 = 'hub' then total_completed_order ELSE NULL END)/CAST(SUM(total_completed_order) AS DOUBLE) AS hub_coverage_case1
       ,SUM(CASE WHEN case2 = 'hub' then total_completed_order ELSE NULL END)/CAST(SUM(total_completed_order) AS DOUBLE) AS hub_coverage_case2
       ,SUM(CASE WHEN case3 = 'hub' then total_completed_order ELSE NULL END)/CAST(SUM(total_completed_order) AS DOUBLE) AS hub_coverage_case3
       ,SUM(CASE WHEN delivered_by = 'hub' then driver_cost ELSE NULL END)/SUM(CASE WHEN delivered_by = 'hub' then total_completed_order ELSE NULL END) AS cpo_hub
       ,SUM(CASE WHEN delivered_by = 'non-hub' then driver_cost ELSE NULL END)/SUM(CASE WHEN delivered_by = 'non-hub' then total_completed_order ELSE NULL END) AS cpo_non_hub
       ,SUM(driver_cost)/SUM(total_completed_order) AS cpo_overall

FROM summary

GROUP BY 1