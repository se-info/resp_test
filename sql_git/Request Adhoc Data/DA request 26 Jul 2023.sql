with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date_trunc('month',date'2022-01-01'),date'2023-07-25') bar
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
,summary AS 
(SELECT 
         p.period
        ,raw.grass_date
        ,date_trunc('month',raw.grass_date) as start_date 
        ,raw.city_name
        ,raw.shipper_id 
        ,raw.shipper_name 
        ,raw.onboard_date
        ,raw.quit_work_date
        ,raw.shipper_status_code
        ,CASE WHEN raw.onboard_date NOT BETWEEN CAST(date_trunc('month',raw.grass_date) AS DATE) AND raw.grass_date AND raw.shipper_status_code = 1 THEN 1 ELSE 0 END AS type1 

        ,CASE WHEN raw.onboard_date NOT BETWEEN CAST(date_trunc('month',raw.grass_date) AS DATE) AND raw.grass_date AND raw.shipper_status_code = 0 THEN 1 ELSE 0 END AS type2

        ,CASE WHEN raw.onboard_date BETWEEN CAST(date_trunc('month',raw.grass_date) AS DATE) AND raw.grass_date THEN 1 ELSE 0 END AS type3

        ,CASE WHEN raw.onboard_date BETWEEN CAST(date_trunc('month',raw.grass_date) AS DATE) AND raw.grass_date 
                   AND shipper_status_code = 0
                   AND raw.quit_work_date BETWEEN CAST(date_trunc('month',raw.grass_date) AS DATE) AND raw.grass_date THEN 1 ELSE 0 END AS type4



FROM raw 

INNER JOIN params_date p 
    on raw.grass_date = p.end_date

-- WHERE raw.grass_date = date'2022-06-30'
order by 4 asc 
)
SELECT 
         period 
        ,city_name
        ,COUNT(DISTINCT CASE WHEN type1 = 1 THEN (shipper_id) ELSE NULL END) AS type1
        ,COUNT(DISTINCT CASE WHEN type2 = 1 THEN (shipper_id) ELSE NULL END) AS type2
        ,COUNT(DISTINCT CASE WHEN type3 = 1 THEN (shipper_id) ELSE NULL END) AS type3
        ,COUNT(DISTINCT CASE WHEN type4 = 1 THEN (shipper_id) ELSE NULL END) AS type4

FROM summary

GROUP BY 1,2