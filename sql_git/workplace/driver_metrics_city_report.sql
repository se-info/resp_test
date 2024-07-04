WITH raw_date AS
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date_trunc('week',current_date) - interval '28' day,current_date - interval '1' day) bar
)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period_,start_date,end_date,days) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,report_date
        ,report_date
        ,1

from raw_date

UNION ALL 
SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,date_trunc('week',report_date) 
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('week',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3

UNION 

SELECT 
        '3. Monthly'
        ,'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('month',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3
) 
,agg_tab AS 
(SELECT 
        shipper_id,
        ARRAY_AGG(DISTINCT report_date) AS agg_delivered_date,
        ARRAY_AGG(DISTINCT CASE WHEN total_order_food > 0 THEN report_date ELSE NULL END) AS agg_delivered_date_delivery,
        ARRAY_AGG(DISTINCT CASE WHEN total_order_spxi > 0 THEN report_date ELSE NULL END) AS agg_delivered_date_spxi

FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab
WHERE total_order > 0 
GROUP BY 1
)
,raw AS 
(SELECT 
        r.report_date,
        r.shipper_id,
        r.shipper_tier,
        CASE 
        WHEN r.city_name IN 
        ('HCM City',
        'Ha Noi City',
        'Da Nang City') THEN city_name
        WHEN r.city_name IN
        ('Dong Nai',
        'Can Tho City',
        'Binh Duong',
        'Hai Phong City',
        'Hue City',
        'Vung Tau',
        'Khanh Hoa') THEN 'T2' 
        WHEN r.city_name IN 
        ('Bac Ninh',
        'Nghe An',
        'Thai Nguyen',
        'Quang Ninh',
        'Lam Dong',
        'Quang Nam') THEN 'T3'  
        ELSE 'new_cities' END AS cities,
        r.total_order,
        r.online_hour,
        r.work_hour,
        IF(r.online_hour > r.work_hour,(r.online_hour - r.work_hour)*1.0000,0) AS down_time,
        COALESCE(CARDINALITY(FILTER(ag.agg_delivered_date, x -> x between r.report_date - interval '29' day and r.report_date)),0) AS agg_a30,
        COALESCE(CARDINALITY(FILTER(ag.agg_delivered_date_delivery, x -> x between r.report_date - interval '29' day and r.report_date)),0) AS agg_a30_delivery,
        COALESCE(CARDINALITY(FILTER(ag.agg_delivered_date_spxi, x -> x between r.report_date - interval '29' day and r.report_date)),0) AS agg_a30_spxi,
        IF(r.total_order > 0,1,0) AS a1,
        IF(r.total_order_food > 0,1,0) AS a1_delivery,
        IF(r.total_order_spxi > 0,1,0) AS a1_spxi,
        r.city_name,
        r.onboard_date
FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab r 
LEFT JOIN agg_tab ag ON ag.shipper_id = r.shipper_id 

WHERE 1 = 1  
AND REGEXP_LIKE(COALESCE(city_name,'n/a'),'n/a|Dien Bien|Test') = False
)
SELECT  
        p.period_,
        city_name,
        SUM(CASE WHEN a1 = 1 THEN total_order ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1 = 1 THEN (shipper_id,report_date) ELSE NULL END) AS ado_driver,
        COUNT(DISTINCT CASE WHEN a1 = 1 THEN (shipper_id,report_date) ELSE NULL END)*1.0000/COUNT(DISTINCT report_date) AS a1,
        COUNT(DISTINCT CASE WHEN agg_a30 > 0 THEN (shipper_id,report_date) ELSE NULL END)*1.0000/COUNT(DISTINCT report_date) as a30,
        SUM(CASE WHEN a1 = 1 THEN online_hour ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1 = 1 THEN (shipper_id,report_date) ELSE NULL END) AS avg_online,
        SUM(CASE WHEN a1 = 1 THEN work_hour ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1 = 1 THEN (shipper_id,report_date) ELSE NULL END) AS avg_work_hour,
        SUM(CASE WHEN a1 = 1 THEN down_time ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1 = 1 THEN (shipper_id,report_date) ELSE NULL END) AS pp_down_time,

        SUM(CASE WHEN a1_delivery = 1 THEN total_order ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1_delivery = 1 THEN (shipper_id,report_date) ELSE NULL END) AS ado_driver_delivery,
        COUNT(DISTINCT CASE WHEN a1_delivery = 1 THEN (shipper_id,report_date) ELSE NULL END)*1.0000/COUNT(DISTINCT report_date) AS a1_delivery,
        COUNT(DISTINCT CASE WHEN agg_a30_delivery > 0 THEN (shipper_id,report_date) ELSE NULL END)*1.0000/COUNT(DISTINCT report_date) as a30_delivery,
        SUM(CASE WHEN a1_delivery = 1 THEN online_hour ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1_delivery = 1 THEN (shipper_id,report_date) ELSE NULL END) AS avg_online_delivery,
        SUM(CASE WHEN a1_delivery = 1 THEN work_hour ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1_delivery = 1 THEN (shipper_id,report_date) ELSE NULL END) AS avg_work_hour_delivery,
        SUM(CASE WHEN a1_delivery = 1 THEN down_time ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1_delivery = 1 THEN (shipper_id,report_date) ELSE NULL END) AS pp_down_time_delivery,

        SUM(CASE WHEN a1_spxi = 1 THEN total_order ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1_spxi = 1 THEN (shipper_id,report_date) ELSE NULL END) AS ado_driver_spxi,
        COUNT(DISTINCT CASE WHEN a1_spxi = 1 THEN (shipper_id,report_date) ELSE NULL END)*1.0000/COUNT(DISTINCT report_date) AS a1_spxi,
        COUNT(DISTINCT CASE WHEN agg_a30_spxi > 0 THEN (shipper_id,report_date) ELSE NULL END)*1.0000/COUNT(DISTINCT report_date) as a30_spxi,
        SUM(CASE WHEN a1_spxi = 1 THEN online_hour ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1_spxi = 1 THEN (shipper_id,report_date) ELSE NULL END) AS avg_online_spxi,
        SUM(CASE WHEN a1_spxi = 1 THEN work_hour ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1_spxi = 1 THEN (shipper_id,report_date) ELSE NULL END) AS avg_work_hour_spxi,
        SUM(CASE WHEN a1_spxi = 1 THEN down_time ELSE NULL END)*1.0000/COUNT(DISTINCT CASE WHEN a1_spxi = 1 THEN (shipper_id,report_date) ELSE NULL END) AS pp_down_time_spxi,

        COUNT(DISTINCT CASE WHEN onboard_date BETWEEN date_trunc('week',report_date) AND date_trunc('week',report_date) + INTERVAL '6' DAY THEN shipper_id ELSE NULL END) AS new_recruitment


FROM raw

INNER JOIN params_date p ON raw.report_date BETWEEN p.start_date AND p.end_date

WHERE 1 = 1 
AND p.period_group = '2. Weekly'
GROUP BY 1,2

-- select * from shopeefood.shopee_foodalgo_assignment_vn_db__driver_status_tab__reg_continuous_s0_live where uid = 41048025