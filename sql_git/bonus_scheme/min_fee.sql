WITH raw AS
(select 
         raw.id AS order_id
        ,CAST(JSON_EXTRACT(dotet.order_data,'$.shipper_policy.type') AS BIGINT) AS driver_policy
        ,raw.city_name 
        ,raw.last_incharge_timestamp
        ,raw.delivered_timestamp
        ,raw.order_status
        ,HOUR(raw.last_incharge_timestamp)*100 + MINUTE(raw.last_incharge_timestamp) AS hour_min
        ,13500 AS original_fee
        ,COALESCE(CAST(mf.min_fee AS BIGINT),0) AS expected_fee
        ,CASE 
              WHEN COALESCE(CAST(mf.min_fee AS BIGINT),0) > 0 THEN COALESCE(CAST(mf.min_fee AS BIGINT),0) - 13500 
              ELSE 0 END AS adjust_fee
        ,dot.delivery_cost/CAST(100 AS DOUBLE) AS check_fee



from dev_vnfdbi_opsndrivers.phong_raw_order_v2 raw 

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = raw.id 
    and dot.ref_order_category = raw.order_type

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet 
    on dot.id = dotet.order_id

left join vnfdbi_opsndrivers.phong_test_table mf 
    on CAST(mf.city_id AS BIGINT) = raw.city_id
    and CAST(mf.report_date AS DATE) = DATE(raw.last_incharge_timestamp)
    and (HOUR(raw.last_incharge_timestamp)*100 + MINUTE(raw.last_incharge_timestamp)) BETWEEN CAST(mf.start_hour AS BIGINT) AND CAST(mf.end_hour AS BIGINT)

WHERE DATE(raw.last_incharge_timestamp) BETWEEN DATE'2023-04-28' AND DATE'2023-05-05'     
AND CAST(JSON_EXTRACT(dotet.order_data,'$.shipper_policy.type') AS BIGINT) = 2 
AND raw.order_status = 'Delivered'
AND raw.source = 'order_food'
AND raw.city_id in (217,218,220)
)
SELECT 
         DATE(last_incharge_timestamp) AS inflow_date
        ,city_name
        ,SUM(adjust_fee) AS adjust_fee


FROM raw


GROUP BY 1,2