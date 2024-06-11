WITH raw AS 
(SELECT  
        YEAR(report_date)*100 + WEEK(report_date) AS year_week,
        raw.shipper_id,
        sm.shipper_name,
        raw.city_name,
        SUM(total_order) AS ado,
        COUNT(DISTINCT report_date) AS working_day
        -- CAST(MIN(report_date) AS VARCHAR)||' - '||CAST(MAX(report_date) AS VARCHAR) AS period



FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab raw 
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = raw.shipper_id 
    and try_cast(sm.grass_date as date) = raw.report_date
-- where sm.shipper_id = 42020592
where 1 = 1 
-- AND raw.city_name in ('Hai Duong','Nam Dinh City')
AND raw.city_name IN ('Kien Giang','Dong Thap','Phu Yen')
AND total_order > 0 
AND report_date BETWEEN DATE'2024-05-27' AND DATE'2024-06-02'
GROUP BY 1,2,3,4
)
select * from
(SELECT 
        *,
        CASE 
        WHEN ado >= 70 THEN 200000
        WHEN ado >= 40 THEN 100000
        ELSE 0 END AS bonus_value
FROM raw)
where bonus_value > 0 


