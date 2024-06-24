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
AND DATE(COALESCE(raw.delivered_timestamp,raw.returned_timestamp)) BETWEEN DATE'2024-06-17' AND CURRENT_DATE - INTERVAL '1' DAY
GROUP BY 1,2,3 )
,m AS
(SELECT 
        f.*,
        d.online_hour,
        dp.sla_rate

FROM f

LEFT JOIN 
(SELECT 
        created,
        uid,
        SUM(online_by_hour*1.00/3600) AS online_hour 

FROM dev_vnfdbi_opsndrivers.driver_ops_driver_supply_tab 
GROUP BY 1,2
)d ON d.created = f.report_date AND d.uid = f.shipper_id 
LEFT JOIN driver_ops_driver_performance_tab dp ON dp.shipper_id = f.shipper_id and dp.report_date = f.report_date
)
SELECT 
        shipper_id,
        city_name,
        SUM(e2c+c2c) AS total_order,
        COUNT(DISTINCT CASE WHEN online_hour >= 8 AND sla_rate >= 95 THEN report_date ELSE NULL END) AS qualified_working_days,
        COUNT(DISTINCT CASE WHEN online_hour >= 8 THEN report_date ELSE NULL END) AS qualified_online,
        COUNT(DISTINCT CASE WHEN sla_rate >= 95 THEN report_date ELSE NULL END) AS qualified_sla,
        CASE 
        WHEN COUNT(DISTINCT CASE WHEN online_hour >= 8 AND sla_rate >= 95 THEN report_date ELSE NULL END) > 5 AND SUM(e2c+c2c) >= 180 then 300000
        WHEN COUNT(DISTINCT CASE WHEN online_hour >= 8 AND sla_rate >= 95 THEN report_date ELSE NULL END) >= 5 AND SUM(e2c+c2c) BETWEEN 150 and 179 then 150000
        WHEN COUNT(DISTINCT CASE WHEN online_hour >= 8 AND sla_rate >= 95 THEN report_date ELSE NULL END) >= 4 AND SUM(e2c+c2c) BETWEEN 120 and 149 then 100000
        ELSE 0 END AS bonus_value

FROM m 
GROUP BY 1,2


