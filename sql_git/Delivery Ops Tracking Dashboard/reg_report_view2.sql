SELECT  
        'M- ' || CAST(YEAR(date_trunc('month',date(from_unixtime(create_time - 3600))))*100 
        + MONTH(date_trunc('month',date_trunc('month',date(from_unixtime(create_time - 3600))))) as varchar) AS year_month
        ,'1. Register' AS metrics 
        ,COUNT(DISTINCT id) AS values_ 

    FROM shopeefood.foody_internal_db__shipper_registration_tab__reg_daily_s0_live

    WHERE 1 = 1 
    AND date(from_unixtime(create_time - 3600)) >= DATE'2022-11-01'
GROUP BY 1 ,2 

UNION ALL 

SELECT 
        'M- ' || CAST(YEAR(date_trunc('month',date(from_unixtime(create_time - 3600))))*100 
        + MONTH(date_trunc('month',date_trunc('month',date(from_unixtime(create_time - 3600))))) as varchar) AS year_month
        ,'2. Onboarded' AS metrics 
        ,COUNT(DISTINCT uid) AS values_ 

FROM shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live
WHERE 1 = 1 
AND date(from_unixtime(create_time - 3600)) >= DATE'2022-11-01'
GROUP BY 1 ,2 

UNION ALL
SELECT 
         year_month
        ,'3. Driver Available' AS metrics 
        ,values_ 
        

FROM 
(SELECT CAST(grass_date AS DATE) AS report_date 
       ,'M- ' || CAST(YEAR(date_trunc('month',CAST(grass_date AS DATE)))*100 
        + MONTH(date_trunc('month',date_trunc('month',CAST(grass_date AS DATE)))) as varchar) AS year_month
       ,ROW_NUMBER()OVER(PARTITION BY YEAR(CAST(grass_date AS DATE))*100+MONTH(CAST(grass_date AS DATE)) ORDER BY CAST(grass_date AS DATE) DESC) AS rank
       ,COUNT(DISTINCT shipper_id) AS values_  


FROM shopeefood.foody_mart__profile_shipper_master sm 
WHERE 1 = 1
AND sm.shipper_status_code = 1 
AND sm.grass_date != 'current'
GROUP BY 1,grass_date )

WHERE rank = 1
AND report_date >= date'2022-11-01'

UNION ALL 

SELECT
        'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 
        + MONTH(date_trunc('month',date_trunc('month',report_date))) as varchar) AS year_month
        ,'4. Active Driver' AS metrics 
        ,COUNT(CASE WHEN online_time > 0 THEN shipper_id ELSE NULL END)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS values_ 

FROM (SELECT
                 report_date
                 ,shipper_id
                 ,SUM(online_time) AS online_time 
                 ,SUM(work_time) AS work_time 
           FROM dev_vnfdbi_opsndrivers.shopeefood_vn_driver_supply_hour_by_time_slot
           GROUP BY 1,2 )


WHERE report_date >= DATE'2022-11-01'
GROUP BY 1 ,2

UNION ALL

SELECT
        'M- ' || CAST(YEAR(date_trunc('month',report_date))*100 
        + MONTH(date_trunc('month',date_trunc('month',report_date))) as varchar) AS year_month
        ,CASE
             WHEN online_time >= 7.5 THEN '4.1 Fulltime Active'
             WHEN sm.shipper_type_id != 12 AND online_time >= 0 AND online_time <= 7.5 THEN '4.2 Parttime Active'
             WHEN sm.shipper_type_id = 12 THEN '4.3 Hub Active' END AS metrics     
        ,COUNT(CASE WHEN online_time > 0 THEN rp.shipper_id ELSE NULL END)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS values_ 

FROM (SELECT
                 report_date
                 ,shipper_id
                 ,SUM(online_time) AS online_time 
                 ,SUM(work_time) AS work_time 
           FROM dev_vnfdbi_opsndrivers.shopeefood_vn_driver_supply_hour_by_time_slot
           GROUP BY 1,2 ) rp

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = rp.shipper_id 
    and try_cast(sm.grass_date as date) = rp.report_date

WHERE report_date >= DATE'2022-11-01'
GROUP BY 1 ,2


