WITH driver_list AS
(SELECT  
       report_date, 
       uid AS shipper_id,
       service_name,
       CARDINALITY(FILTER(service_name,x ->x in ('Delivery') )) as delivery_service_filter,
       CARDINALITY(FILTER(service_name,x ->x in ('Now Ship','Ship Shopee') )) as ship_service_filter

FROM dev_vnfdbi_opsndrivers.driver_ops_driver_services_tab 

WHERE 1 = 1
AND report_date BETWEEN CURRENT_DATE - INTERVAL '90' DAY AND CURRENT_DATE - INTERVAL '1' DAY
)
,f AS
(SELECT 
       DATE(COALESCE(raw.delivered_timestamp,raw.returned_timestamp)) AS report_date, 
       raw.shipper_id,
       raw.city_name,
       COUNT(DISTINCT CASE WHEN raw.order_type = 6 THEN order_code ELSE NULL END) AS e2c,
       COUNT(DISTINCT CASE WHEN raw.order_type != 6 THEN order_code ELSE NULL END) AS c2c


FROM dev_vnfdbi_opsndrivers.driver_ops_raw_order_tab raw 

INNER JOIN (SELECT * FROM driver_list WHERE ship_service_filter > 0 AND delivery_service_filter = 0) dl 
        ON dl.shipper_id = raw.shipper_id
        AND dl.report_date = DATE(COALESCE(raw.delivered_timestamp,raw.returned_timestamp))

WHERE 1 = 1 
AND raw.order_type != 0 
AND raw.order_status IN ('Delivered')
AND DATE(COALESCE(raw.delivered_timestamp,raw.returned_timestamp)) BETWEEN CURRENT_DATE - INTERVAL '1' DAY AND CURRENT_DATE - INTERVAL '1' DAY
GROUP BY 1,2,3 )
SELECT  
        f.*,
        it.total_ship_shared,
        it.total_other_income,
        it.driver_daily_bonus,
        dp.online_hour,
        dp.work_hour,
        dp.sla_rate


FROM f

LEFT JOIN driver_ops_driver_income_tracking_tab it ON it.shipper_id = f.shipper_id AND it.report_date = f.report_date

LEFT JOIN driver_ops_driver_performance_tab dp ON dp.shipper_id  = f.shipper_id AND dp.report_date = f.report_date


