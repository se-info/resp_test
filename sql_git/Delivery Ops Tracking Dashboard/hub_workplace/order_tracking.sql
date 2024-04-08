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
,raw AS 
(SELECT 
         dot.ref_order_id
        ,dot.ref_order_code 
        ,DATE(FROM_UNIXTIME(dot.submitted_time - 3600)) as created_date
        ,dot.uid
        ,HOUR(FROM_UNIXTIME(dot.submitted_time - 3600)) as create_hour
        ,dot.delivery_distance/CAST(1000 AS DOUBLE) as distance
        -- ,sm.shipper_type_id
        ,city.name_en AS city_name
        ,COALESCE(CAST(json_extract(dotet.order_data,'$.hub_id') as BIGINT),0) as hub_id
        ,COALESCE(CAST(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id
        ,COALESCE(CAST(json_extract(dotet.order_data,'$.drop_hub_id') as BIGINT ),0) as drop_hub_id

FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE date(dt) = current_date - interval '1' day) dot

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da WHERE date(dt) = current_date - interval '1' day) dotet 
    ON dot.id = dotet.order_id

-- LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
--     on sm.shipper_id = dot.uid and TRY_CAST(sm.grass_date AS DATE) = date(from_unixtime(dot.submitted_time - 3600))

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
    on city.id = dot.pick_city_id
    and city.country_id = 86    

WHERE 1 = 1
AND DATE(FROM_UNIXTIME(dot.submitted_time - 3600)) BETWEEN date_trunc('month',current_date) - interval '60' day AND current_date - interval '1' day
AND dot.ref_order_category = 0 
AND dot.order_status = 400
)
,summary AS 
(SELECT 
         created_date
        ,city_name
        ,COUNT(DISTINCT ref_order_code) AS total_order  
        ,COUNT(DISTINCT CASE WHEN hub_id > 0 THEN ref_order_code ELSE NULL END) AS total_hub_order 
        ,COUNT(DISTINCT CASE WHEN pick_hub_id > 0 THEN ref_order_code ELSE NULL END) AS total_district


FROM raw
GROUP BY 1,2 
)
SELECT 
       p.period_group 
      ,p.period

      ,SUM(total_order)/CAST(p.days AS DOUBLE) AS vn_order
      ,SUM(CASE WHEN city_name = 'HCM City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_order 
      ,SUM(CASE WHEN city_name = 'Ha Noi City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_order 
      ,SUM(CASE WHEN city_name = 'Hai Phong City' THEN total_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_order 

      ,SUM(total_hub_order)/CAST(p.days AS DOUBLE) AS vn_order_qualified
      ,SUM(CASE WHEN city_name = 'HCM City' THEN total_hub_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_order_qualified 
      ,SUM(CASE WHEN city_name = 'Ha Noi City' THEN total_hub_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_order_qualified 
      ,SUM(CASE WHEN city_name = 'Hai Phong City' THEN total_hub_order ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_order_qualified      
        
      ,SUM(total_district)/CAST(p.days AS DOUBLE) AS vn_order_district
      ,SUM(CASE WHEN city_name = 'HCM City' THEN total_district ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_order_district
      ,SUM(CASE WHEN city_name = 'Ha Noi City' THEN total_district ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_order_district 
      ,SUM(CASE WHEN city_name = 'Hai Phong City' THEN total_district ELSE NULL END)/CAST(p.days AS DOUBLE) AS hcm_order_district 
      ,p.days  
FROM summary s

INNER JOIN params_date p 
    on s.created_date between p.start_date and p.end_date

GROUP BY 1,2,p.days        