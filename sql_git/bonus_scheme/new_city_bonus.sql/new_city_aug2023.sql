WITH raw AS 
(SELECT  
        YEAR(report_date)*100 + WEEK(report_date) AS year_week,
        raw.shipper_id,
        sm.shipper_name,
        raw.city_name,
        SUM(total_order) AS ado,
        COUNT(DISTINCT report_date) AS working_day,
        CAST(MIN(report_date) AS VARCHAR)||' - '||CAST(MAX(report_date) AS VARCHAR) AS period



FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab raw 
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = raw.shipper_id 
    and try_cast(sm.grass_date as date) = raw.report_date
WHERE REGEXP_LIKE(LOWER(raw.city_name),'binh thuan|binh dinh') = true
AND total_order > 0 
AND report_date BETWEEN DATE'2023-08-21' AND DATE'2023-08-27'
GROUP BY 1,2,3,4
)
SELECT 
        *,
        CASE 
        WHEN ado >= 50 THEN 200000
        WHEN ado >= 30 THEN 100000
        ELSE 0 END AS bonus_value



FROM raw
;
SELECT 
        city_name,
        COUNT(DISTINCT shipper_id) AS onb 

FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab raw 
WHERE REGEXP_LIKE(LOWER(raw.city_name),'binh thuan|binh dinh') = true
AND raw.report_date = current_date - interval '1' day
AND raw.onboard_date <= DATE'2023-09-03' 
GROUP BY 1