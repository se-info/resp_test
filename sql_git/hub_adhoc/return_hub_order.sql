WITH return_order_tab AS 
(SELECT 
        FROM_UNIXTIME(raw.create_time - 3600) AS created,
        raw.*,
        dot.ref_order_id 
FROM shopeefood.shopeefood_mart_dwd_vn_assign_shipper_batch_processing_log_di raw 
LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE date(dt) = current_date - interval '1' day) dot 
    ON CAST(raw.driver_order_id AS BIGINT)  = dot.id  
WHERE grass_date >= DATE'2023-09-14'
-- AND city_id = 217
)
,metrics AS 
(SELECT 
        raw.report_date,
        raw.city_name,
        raw.ref_order_code,
        CASE 
        WHEN raw.hub_id > 0 THEN 1 ELSE 0 END AS is_hub_qualified,
        CASE 
        WHEN (raw.hub_id > 0 OR raw.is_hub = 1) AND pick_hub_id = drop_hub_id THEN 1
        WHEN (raw.hub_id > 0 OR raw.is_hub = 1) AND pick_hub_id != drop_hub_id THEN 2
        ELSE 0 END AS pick_rule,
        rot.is_recall_hub,
        raw.is_hub

FROM phong_hub_temp_table raw 

-- # return hub tab
LEFT JOIN return_order_tab rot  
    on rot.ref_order_id = raw.ref_order_id

LEFT JOIN return_order_tab rot_filter
    on rot_filter.driver_order_id = rot.driver_order_id
    and rot_filter.created > rot.created

WHERE 1 = 1 
AND rot_filter.created IS NULL 
AND (report_date BETWEEN DATE'2023-09-15' AND DATE'2023-09-16'
    OR 
    report_date BETWEEN DATE'2023-08-25' AND DATE'2023-08-26'
    )
AND city_name = 'HCM City'
)
SELECT
        report_date,
        city_name,
        COUNT(DISTINCT CASE WHEN is_hub_qualified = 1 THEN ref_order_code ELSE NULL END) AS total_hub_qualified,
        COUNT(DISTINCT CASE WHEN is_hub_qualified = 1 AND pick_rule = 1 THEN ref_order_code ELSE NULL END) AS total_hub_qualified_same_pick_drop,
        COUNT(DISTINCT CASE WHEN is_hub_qualified = 1 AND pick_rule = 2 THEN ref_order_code ELSE NULL END) AS total_hub_qualified_diff_pick_drop,
        COUNT(DISTINCT CASE WHEN is_hub = 1 THEN ref_order_code ELSE NULL END) AS total_hub_delivered,
        COUNT(DISTINCT CASE WHEN is_hub = 1 AND pick_rule = 1 AND (is_recall_hub != 'true' OR is_recall_hub IS NULL) THEN ref_order_code ELSE NULL END) AS total_hub_delivered_same_pick_drop,
        COUNT(DISTINCT CASE WHEN is_hub = 1 AND pick_rule = 2 AND (is_recall_hub != 'true' OR is_recall_hub IS NULL) THEN ref_order_code ELSE NULL END) AS total_hub_delivered_diff_pick_drop,
        COUNT(DISTINCT CASE WHEN is_hub = 1 AND is_hub_qualified = 1 AND is_recall_hub = 'true' THEN ref_order_code ELSE NULL END) AS hub_return_qualified,
        COUNT(DISTINCT CASE WHEN is_hub = 1 AND is_hub_qualified = 0 AND is_recall_hub = 'true' THEN ref_order_code ELSE NULL END) AS hub_return_non_qualified



FROM metrics
GROUP BY 1,2