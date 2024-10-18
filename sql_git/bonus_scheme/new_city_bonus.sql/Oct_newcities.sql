WITH pay_note(week_num,max_date,min_date) as 
(SELECT
        year(report_date)*100 + week(report_date),
        max(report_date),
        min(report_date)
FROM
(
(SELECT SEQUENCE(date_trunc('week',current_date)- interval '2' month,date_trunc('week',current_date) - interval '1' day) bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
group by 1 
) 
,raw AS 
(SELECT  
        YEAR(report_date)*100 + WEEK(report_date) AS year_week,
        raw.shipper_id,
        sm.shipper_name,
        raw.city_name,
        'spf_do_0014|NON_HUB_NEW_DRIVER_Thu nhap tuan nang suat_'||date_format(pn.min_date,'%Y-%m-%d')||'_'|| date_format(pn.max_date,'%Y-%m-%d') as note,
        SUM(total_order) AS ado,
        COUNT(DISTINCT report_date) AS working_day

FROM dev_vnfdbi_opsndrivers.driver_ops_driver_performance_tab raw 
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = raw.shipper_id 
    and try_cast(sm.grass_date as date) = raw.report_date

LEFT JOIN pay_note pn 
    on pn.week_num = (year(raw.report_date)*100 + week(raw.report_date))

where 1 = 1 

AND raw.city_name IN
('Binh Phuoc',
'Tay Ninh',
'Ninh Thuan',
'Gia Lai') 

AND total_order > 0 
AND report_date BETWEEN DATE'2024-10-01' AND DATE'2024-10-13'
GROUP BY 1,2,3,4,5
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

