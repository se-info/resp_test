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

WHERE 1 = 1 
AND REGEXP_LIKE(city_name,'Dien Bien|Test|Stres') = FALSE
)
,a1_a30 as 
(SELECT 
        date_trunc('month',report_date) as month_,
        SUM(a1)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS a1,
        SUM(a30)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS a30,
        SUM(a60)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS a60,
        SUM(a90)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS a90,
        SUM(a120)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS a120

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
WHERE (report_date >= DATE'2023-06-01' AND report_date <= DATE'2023-06-30'
or report_date >= DATE'2024-06-01' AND report_date <= DATE'2024-06-30')
group by 1
)
,raw AS
(SELECT 
        DATE_TRUNC('month',ic.date_) AS month_,
        ic.date_,
        ic.current_driver_tier,
        ic.partner_id,
        ic.total_earning_before_tax,
        ic.total_earning_hub,
        ic.total_earning_non_hub,
        ic.city_name_full,
        ic.total_bill,
        (ic.total_bill_food + ic.total_bill_market) AS total_delivery_ado,
        (ic.total_bill_now_ship + ic.total_bill_now_ship_shopee + ic.total_bill_now_ship_instant 
                + ic.total_bill_now_ship_food_merchant + ic.total_bill_now_ship_sameday) AS total_spxi_ado

FROM vnfdbi_opsndrivers.snp_foody_shipper_income_tab ic

WHERE (ic.date_ >= DATE'2023-06-01' AND ic.date_ <= DATE'2023-06-30'
or ic.date_ >= DATE'2024-06-01' AND ic.date_ <= DATE'2024-06-30')
)
SELECT 
        raw.month_,
        a.a1,
        a.a30,
        a.a60,
        a.a90,
        SUM(raw.total_bill)/CAST(COUNT(DISTINCT raw.date_) AS DOUBLE) AS total_ado,
        SUM(raw.total_delivery_ado)/CAST(COUNT(DISTINCT raw.date_) AS DOUBLE) AS total_delivery_ado,
        SUM(raw.total_spxi_ado)/CAST(COUNT(DISTINCT raw.date_) AS DOUBLE) AS total_spxi_ado,
        AVG(total_earning_before_tax) AS daily_income,
        AVG(total_earning_hub) AS daily_income_hub,
        AVG(total_earning_non_hub) AS daily_income_non_hub


FROM raw 

LEFT JOIN a1_a30 a ON a.month_ = raw.month_

GROUP BY 1,2,3,4,5


