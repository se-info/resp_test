WITH raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date_trunc('month',date'2022-01-01'),date'2023-07-31') bar
)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period,start_date,end_date,days) AS 
(SELECT 
         'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 + MONTH(date_trunc('month',report_date)) as varchar)
        ,date_trunc('month',report_date) 
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('month',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2
)
,raw AS 
(SELECT 
        TRY_CAST(sm.grass_date AS DATE) AS grass_date
       ,sm.shipper_id 
       ,sp.full_name AS shipper_name 
       ,sp.city_id
       ,CASE 
        WHEN sp.city_id = 217 then 'T1' 
        WHEN sp.city_id = 218 then 'T1' 
        WHEN sp.city_id = 219 then 'T1' 
        WHEN sp.city_id = 222 then 'T2' 
        WHEN sp.city_id = 273 then 'T2' 
        WHEN sp.city_id = 221 then 'T2'
        WHEN sp.city_id = 230 then 'T2'
        WHEN sp.city_id = 220 then 'T2'
        WHEN sp.city_id = 223 then 'T2'
        WHEN sp.city_id = 248 then 'T3'
        WHEN sp.city_id = 271 then 'T3'
        WHEN sp.city_id = 257 then 'T3'
        WHEN sp.city_id = 228 then 'T3'
        WHEN sp.city_id = 254 then 'T3'
        WHEN sp.city_id = 265 then 'T3'
        WHEN sp.city_id = 263 then 'T3'
        WHEN sp.city_id = 238 then 'T3'
        WHEN sp.city_id = 272 then 'T3'
        END AS city_tier
       ,city.name_en AS city_name
       ,sm.shipper_status_code
       ,DATE(FROM_UNIXTIME(sp.create_time - 3600)) AS onboard_date 
       ,DATE(FROM_UNIXTIME(sp.termination_date - 3600)) AS quit_work_date 


FROM shopeefood.foody_mart__profile_shipper_master sm 

LEFT JOIN shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live sp 
    on sp.uid = sm.shipper_id

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
    on city.id = sp.city_id
    and city.country_id = 86
WHERE regexp_like(LOWER(sm.city_name),'test|Test|TEST|Dien Bien|dien bien') = false 
AND regexp_like(LOWER(sp.personal_email),'shopee|foody|gofast') = false
AND regexp_like(LOWER(sp.full_name),'test|Test|TEST|Dien Bien|dien bien') = false
AND sm.shipper_type_id != 3
)
SELECT 
         YEAR(grass_date)*100 + MONTH(grass_date) AS report_month
        ,type_
        ,COUNT(DISTINCT shipper_id) AS total_onboard
        ,COUNT(DISTINCT CASE WHEN is_activated = 1 THEN shipper_id ELSE NULL END) AS total_activated_in_month
        ,COUNT(DISTINCT CASE WHEN is_activated_l7d = 1 THEN shipper_id ELSE NULL END) AS total_activated_in_l7d
        ,COUNT(DISTINCT CASE WHEN is_activated = 1 THEN shipper_id ELSE NULL END)/CAST(COUNT(DISTINCT shipper_id) AS DOUBLE) AS pct_activated
        ,COUNT(DISTINCT CASE WHEN is_online_l7d = 1 THEN shipper_id ELSE NULL END)/CAST(COUNT(DISTINCT shipper_id) AS DOUBLE) AS pct_online_l7d
        ,COUNT(DISTINCT CASE WHEN is_activated_l7d = 1 THEN shipper_id ELSE NULL END)/CAST(COUNT(DISTINCT shipper_id) AS DOUBLE) AS pct_activated_l7d

FROM        
(SELECT     
         raw.* 
        ,CASE 
         WHEN city_tier IN ('T1','T2') THEN 'hybrid'
         ELSE 'online' END AS type_  
        ,check.min_order_date
        ,raw.onboard_date + interval '7' day AS check
        ,CASE WHEN check.min_order_date IS NOT NULL THEN 1 ELSE 0 END AS is_activated
        ,CASE WHEN check.min_online_date BETWEEN raw.onboard_date AND raw.onboard_date + interval '6' day THEN 1 ELSE 0 END AS is_online_l7d
        ,CASE WHEN check.min_order_date BETWEEN raw.onboard_date AND raw.onboard_date + interval '6' day THEN 1 ELSE 0 END AS is_activated_l7d
        -- ,CASE WHEN YEAR(raw.onboard_date)*100 + WEEK(raw.onboard_date) = YEAR(raw.grass_date)*100 + WEEK(raw.grass_date) THEN 1 ELSE 0 END AS is_onboard_in_month



FROM raw 

LEFT JOIN 
(SELECT 
         uid
        ,MIN(DATE(FROM_UNIXTIME(report_date - 3600))) AS min_online_date
        ,MIN(CASE WHEN total_completed_order > 0 THEN DATE(FROM_UNIXTIME(report_date - 3600)) ELSE NULL END) AS min_order_date

FROM shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live
WHERE 1 = 1 

GROUP BY 1 
    ) check on check.uid = raw.shipper_id



INNER JOIN params_date p 
    on raw.grass_date = p.end_date

WHERE 1 = 1 AND onboard_date >= DATE'2023-06-01')

-- LIMIT 20 
GROUP BY 1,2
;
SELECT 
        created_year_month,
        IF(city_tier IN ('T1','T2'),'hybrid','online') AS type_,
        SUM(total_order)*1.00/SUM(working_day) AS ado_blended
FROM
(SELECT
        YEAR(report_date)*100 + MONTH(report_date) AS created_year_month,
        YEAR(onboard_date)*100 + MONTH(onboard_date) AS onboard_year_month,
        CASE
        WHEN (YEAR(report_date)*100 + MONTH(report_date)) = (YEAR(onboard_date)*100 + MONTH(onboard_date)) THEN 1
        ELSE 0 END AS flag,
        CASE
        WHEN city_name ='HCM City' THEN 'T1'
        WHEN city_name ='Ha Noi City' THEN 'T1'
        WHEN city_name ='Da Nang City' THEN 'T1'
        WHEN city_name ='Binh Duong' THEN 'T2'
        WHEN city_name ='Can Tho City' THEN 'T2'
        WHEN city_name ='Dong Nai' THEN 'T2'
        WHEN city_name ='Hai Phong City' THEN 'T2'
        WHEN city_name ='Hue City' THEN 'T2'
        WHEN city_name ='Vung Tau' THEN 'T2'
        WHEN city_name ='Bac Ninh' THEN 'T3'
        WHEN city_name ='Khanh Hoa' THEN 'T3'
        WHEN city_name ='Lam Dong' THEN 'T3'
        WHEN city_name ='Nghe An' THEN 'T3'
        WHEN city_name ='Quang Nam' THEN 'T3'
        WHEN city_name ='Quang Ninh' THEN 'T3'
        WHEN city_name ='Thai Nguyen' THEN 'T3'
        WHEN city_name ='Thanh Hoa' THEN 'T3'
        WHEN city_name ='Dak Lak' THEN 'T3' END AS city_tier,
        shipper_id,
        SUM(total_order) AS total_order, 
        COUNT(DISTINCT report_date) AS working_day

FROM phong_driver_performance_raw
WHERE onboard_date >= DATE'2023-06-01'
AND REGEXP_LIKE(LOWER(city_name),'test|dien bien') = false
AND total_order > 0
GROUP BY 1,2,3,4,5)
WHERE flag = 1

GROUP BY 1,2


