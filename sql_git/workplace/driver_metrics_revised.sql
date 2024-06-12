WITH raw_date AS
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date_trunc('month',current_date) - interval '2' month,current_date - interval '1' day) bar
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
        ARRAY_AGG(DISTINCT report_date) AS agg_delivered_date
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
        COALESCE(CARDINALITY(FILTER(ag.agg_delivered_date, x -> x between r.report_date - interval '29' day and r.report_date)),0) AS agg_a30

FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab r 
LEFT JOIN agg_tab ag ON ag.shipper_id = r.shipper_id 

WHERE 1 = 1 
AND r.total_order > 0 
AND REGEXP_LIKE(COALESCE(city_name,'n/a'),'n/a|Dien Bien|Test') = False
)
SELECT  
        p.period_,
        COALESCE(cities,'VN') AS cities,
        COALESCE(shipper_tier,'All') AS tier,
        SUM(total_order)*1.0000/COUNT(DISTINCT (shipper_id,report_date)) AS ado_driver,
        SUM(online_hour)*1.0000/COUNT(DISTINCT (shipper_id,report_date)) AS avg_online,
        SUM(work_hour)*1.0000/COUNT(DISTINCT (shipper_id,report_date)) AS avg_work_hour,
        SUM(down_time)*1.0000/SUM(CASE WHEN down_time > 0 THEN online_hour ELSE NULL END) AS pp_down_time


FROM raw

INNER JOIN params_date p ON raw.report_date BETWEEN p.start_date AND p.end_date

WHERE 1 = 1 
GROUP BY 1,GROUPING SETS(cities,shipper_tier,(cities,shipper_tier),())
UNION ALL 
SELECT  
        p.period_,
        'HCM & HN' AS cities,
        COALESCE(shipper_tier,'All') AS tier,
        SUM(total_order)*1.0000/COUNT(DISTINCT (shipper_id,report_date)) AS ado_driver,
        SUM(online_hour)*1.0000/COUNT(DISTINCT (shipper_id,report_date)) AS avg_online,
        SUM(work_hour)*1.0000/COUNT(DISTINCT (shipper_id,report_date)) AS avg_work_hour,
        SUM(down_time)*1.0000/SUM(CASE WHEN down_time > 0 THEN online_hour ELSE NULL END) AS pp_down_time


FROM raw

INNER JOIN params_date p ON raw.report_date BETWEEN p.start_date AND p.end_date

WHERE 1 = 1 
AND cities IN ('HCM City','Ha Noi City')
GROUP BY 1,2,GROUPING SETS(shipper_tier,())
