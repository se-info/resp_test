with agg_driver AS 
(SELECT
        shipper_id,
        ARRAY_AGG( DISTINCT
        CASE
        WHEN total_order > 0 THEN report_date ELSE NULL END            
        ) AS agg_delivered_date
FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab

WHERE total_order > 0
GROUP BY 1 
)
,metrics AS 
(SELECT 
        dp.report_date,
        dp.shipper_id,
        dp.city_name,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date, x -> x between dp.report_date - interval '29' day and dp.report_date)),0) AS agg_a30,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date, x -> x between dp.report_date - interval '59' day and dp.report_date)),0) AS agg_a60,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date, x -> x between dp.report_date - interval '89' day and dp.report_date)),0) AS agg_a90,
        COALESCE(CARDINALITY(FILTER(agg.agg_delivered_date, x -> x between dp.report_date - interval '119' day and dp.report_date)),0) AS agg_a120,
        dp.online_hour,
        dp.work_hour,
        dp.online_hour - dp.work_hour AS down_hour,
        dp.total_order,
        CASE 
        WHEN city_name IN ('HCM City', 'Ha Noi City', 'Da Nang City') THEN city_name
        WHEN city_name IN ('Hai Phong City', 'Hue City', 'Can Tho City', 'Dong Nai', 'Binh Duong', 'Vung Tau') THEN 'T2'
        ELSE 'T3' END AS city_group


FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab dp 

LEFT JOIN agg_driver agg 
    on agg.shipper_id = dp.shipper_id

WHERE 1 = 1 )

SELECT 
        date_trunc('month',report_date) as period,
        -- report_date,
        SUM(a1)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS a1,
        SUM(a30)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS a30,
        -- SUM(a60)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS a60,
        -- SUM(a90)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS a90,
        -- SUM(a120)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS a120,
        SUM(a1)/CAST(SUM(a30) AS DOUBLE) AS a1_a30,
        SUM(total_order)/CAST(SUM(a1) AS DOUBLE) AS ado,
        SUM(online_hour)/CAST(SUM(a1) AS DOUBLE) AS online_hour,
        SUM(work_hour)/CAST(SUM(a1) AS DOUBLE) AS work_hour,
        SUM(down_hour)/CAST(SUM(online_hour) AS DOUBLE) AS pct_down_time

FROM 
(SELECT 
        m.report_date,
        city_group,
        COUNT(DISTINCT CASE WHEN m.total_order > 0 THEN m.shipper_id ELSE NULL END) AS a1, 
        COUNT(DISTINCT CASE WHEN m.agg_a30 > 0 THEN m.shipper_id ELSE NULL END) AS a30, 
        COUNT(DISTINCT CASE WHEN m.agg_a60 > 0 THEN m.shipper_id ELSE NULL END) AS a60, 
        COUNT(DISTINCT CASE WHEN m.agg_a90 > 0 THEN m.shipper_id ELSE NULL END) AS a90, 
        COUNT(DISTINCT CASE WHEN m.agg_a120 > 0 THEN m.shipper_id ELSE NULL END) AS a120, 
        COUNT(DISTINCT CASE WHEN m.total_order > 0 THEN m.shipper_id ELSE NULL END)/ 
        CAST(COUNT(DISTINCT CASE WHEN m.agg_a30 > 0 THEN m.shipper_id ELSE NULL END) AS DOUBLE)
        AS pct_a1_a30,
        COUNT(DISTINCT CASE WHEN m.online_hour > 0 THEN m.shipper_id ELSE NULL END) AS online_driver,
        SUM(total_order) AS total_order,
        SUM(online_hour) AS online_hour,
        SUM(work_hour) AS work_hour,
        SUM(down_hour) AS down_hour,
        SUM(down_hour)/CAST(SUM(online_hour) AS DOUBLE) AS pct_down_hour
FROM metrics m 
GROUP BY 1,2 
) m
where report_date between date'2022-12-01' and date'2023-12-31'
group by 1