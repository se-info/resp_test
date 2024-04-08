with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date_trunc('month',current_date) - interval '60' day,current_date - interval '1' day) bar
)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date,days) as 
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
,raw_v1 AS 
(SELECT 
         date_ AS report_date
        ,uid AS shipper_id
        ,slot_id
        ,hub_type
        ,city_name
        ,registered_
        ,total_order
        ,total_income
        ,extra_ship
        ,daily_bonus
        ,kpi


FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics
WHERE 1 = 1
AND total_order > 0
AND date_ BETWEEN current_date - interval '60' day and current_date - interval '1' day
)
,raw_v2 AS
(SELECT 
         date_ AS report_date
        ,uid AS shipper_id
        ,city_name
        ,COUNT(slot_id) AS total_slot
        ,SUM(total_order) AS total_order
        ,SUM(total_income) AS total_income
        ,SUM(extra_ship) AS extra_ship
        ,SUM(daily_bonus) AS daily_bonus
        ,SUM(kpi) AS kpi


FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics
WHERE 1 = 1
AND total_order > 0
AND date_ BETWEEN current_date - interval '60' day and current_date - interval '1' day

GROUP BY 1,2,3  
)
,metrics AS 
(SELECT 
      report_date
     ,hub_type
     ,city_name
     ,CAST(COUNT(CASE WHEN total_order > 0 THEN shipper_id ELSE NULL END) AS DOUBLE) AS active_
     ,SUM(CASE WHEN total_order > 0 THEN total_income ELSE NULL END) AS avg_income
     ,COUNT(CASE WHEN kpi > 0 THEN shipper_id ELSE NULL END) AS pct_kpi
     ,COUNT(CASE WHEN extra_ship > 0 THEN shipper_id ELSE NULL END) AS pct_compensated
     ,0 AS multi_check



FROM raw_v1

GROUP BY 1,2,3

UNION ALL 

SELECT 
      report_date
     ,'All' AS hub_type
     ,city_name
     ,CAST(COUNT(DISTINCT CASE WHEN total_order > 0 THEN shipper_id ELSE NULL END) AS DOUBLE) AS active_
     ,SUM(CASE WHEN total_order > 0 THEN total_income ELSE NULL END) AS avg_income
     ,COUNT(DISTINCT CASE WHEN kpi > 0 THEN shipper_id ELSE NULL END) AS pct_kpi
     ,COUNT(DISTINCT CASE WHEN extra_ship > 0 THEN shipper_id ELSE NULL END) AS pct_compensated
     ,COUNT(DISTINCT CASE WHEN total_slot > 1 THEN shipper_id ELSE NULL END) AS multi_check   


FROM raw_v2

GROUP BY 1,2,3
)
SELECT
        p.period_group
       ,p.period

       ,SUM(CASE WHEN hub_type = 'All' then avg_income ELSE NULL END)/CAST(SUM(CASE WHEN hub_type = 'All' then active_ ELSE NULL END) AS DOUBLE) AS vn_income_per_driver
       ,SUM(CASE WHEN hub_type != 'All' then avg_income ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' then active_ ELSE NULL END) AS DOUBLE) AS vn_income_per_slot
       ,SUM(CASE WHEN hub_type = 'All' then multi_check ELSE NULL END)/CAST(SUM(CASE WHEN hub_type = 'All' then active_ ELSE NULL END) AS DOUBLE) AS vn_multi
       ,SUM(CASE WHEN hub_type != 'All' then pct_compensated ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' then active_ ELSE NULL END) AS DOUBLE) AS vn_pct_compensated
       ,SUM(CASE WHEN hub_type != 'All' then pct_kpi ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' then active_ ELSE NULL END) AS DOUBLE) AS vn_kpi

       ,SUM(CASE WHEN hub_type = 'All' and city_name = 'HCM City' then avg_income ELSE NULL END)/CAST(SUM(CASE WHEN hub_type = 'All' and city_name = 'HCM City' then active_ ELSE NULL END) AS DOUBLE) AS hcm_income_per_driver
       ,SUM(CASE WHEN hub_type != 'All' and city_name = 'HCM City' then avg_income ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' and city_name = 'HCM City' then active_ ELSE NULL END) AS DOUBLE) AS hcm_income_per_slot
       ,SUM(CASE WHEN hub_type = 'All' and city_name = 'HCM City' then multi_check ELSE NULL END)/CAST(SUM(CASE WHEN hub_type = 'All' and city_name = 'HCM City' then active_ ELSE NULL END) AS DOUBLE) AS hcm_multi
       ,SUM(CASE WHEN hub_type != 'All' and city_name = 'HCM City' then pct_compensated ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' and city_name = 'HCM City' then active_ ELSE NULL END) AS DOUBLE) AS hcm_pct_compensated
       ,SUM(CASE WHEN hub_type != 'All' and city_name = 'HCM City' then pct_kpi ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' and city_name = 'HCM City' then active_ ELSE NULL END) AS DOUBLE) AS hcm_kpi

       ,SUM(CASE WHEN hub_type = 'All' and city_name = 'Ha Noi City' then avg_income ELSE NULL END)/CAST(SUM(CASE WHEN hub_type = 'All' and city_name = 'Ha Noi City' then active_ ELSE NULL END) AS DOUBLE) AS hn_income_per_driver
       ,SUM(CASE WHEN hub_type != 'All' and city_name = 'Ha Noi City' then avg_income ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' and city_name = 'Ha Noi City' then active_ ELSE NULL END) AS DOUBLE) AS hn_income_per_slot
       ,SUM(CASE WHEN hub_type = 'All' and city_name = 'Ha Noi City' then multi_check ELSE NULL END)/CAST(SUM(CASE WHEN hub_type = 'All' and city_name = 'Ha Noi City' then active_ ELSE NULL END) AS DOUBLE) AS hn_multi
       ,SUM(CASE WHEN hub_type != 'All' and city_name = 'Ha Noi City' then pct_compensated ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' and city_name = 'Ha Noi City' then active_ ELSE NULL END) AS DOUBLE) AS hn_pct_compensated
       ,SUM(CASE WHEN hub_type != 'All' and city_name = 'Ha Noi City' then pct_kpi ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' and city_name = 'Ha Noi City' then active_ ELSE NULL END) AS DOUBLE) AS hn_kpi

       ,SUM(CASE WHEN hub_type = 'All' and city_name = 'Hai Phong City' then avg_income ELSE NULL END)/CAST(SUM(CASE WHEN hub_type = 'All' and city_name = 'Hai Phong City' then active_ ELSE NULL END) AS DOUBLE) AS hp_income_per_driver
       ,SUM(CASE WHEN hub_type != 'All' and city_name = 'Hai Phong City' then avg_income ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' and city_name = 'Hai Phong City' then active_ ELSE NULL END) AS DOUBLE) AS hp_income_per_slot
       ,SUM(CASE WHEN hub_type = 'All' and city_name = 'Hai Phong City' then multi_check ELSE NULL END)/CAST(SUM(CASE WHEN hub_type = 'All' and city_name = 'Hai Phong City' then active_ ELSE NULL END) AS DOUBLE) AS hp_multi
       ,SUM(CASE WHEN hub_type != 'All' and city_name = 'Hai Phong City' then pct_compensated ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' and city_name = 'Hai Phong City' then active_ ELSE NULL END) AS DOUBLE) AS hp_pct_compensated
       ,SUM(CASE WHEN hub_type != 'All' and city_name = 'Hai Phong City' then pct_kpi ELSE NULL END)/CAST(SUM(CASE WHEN hub_type != 'All' and city_name = 'Hai Phong City' then active_ ELSE NULL END) AS DOUBLE) AS hp_kpi

       ,p.days 
FROM metrics m 

INNER JOIN params_date p
    on m.report_date BETWEEN p.start_date and p.end_date

GROUP BY 1,2,p.days