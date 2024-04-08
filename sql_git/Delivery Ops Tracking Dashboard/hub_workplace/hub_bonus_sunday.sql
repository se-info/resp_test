WITH params(report_date,uid) AS
(VALUES
(date'2023-03-19',40157722)
) 
,check_qualified_sun AS
(SELECT 
        YEAR(a.date_)*100 + WEEK(a.date_) AS created_year_week 
       ,a.uid AS shipper_id 
       ,SUM(a.registered_) AS total_reg
       ,COUNT(DISTINCT CASE WHEN total_order > 0 then (a.date_,a.slot_id) ELSE NULL END) AS working_day 
       ,CASE 
            WHEN SUM(a.registered_) = COUNT(DISTINCT CASE WHEN total_order > 0 then (a.date_,a.slot_id) ELSE NULL END) THEN 1 
            ELSE 0 END AS is_qualified
FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics a 
WHERE 1 = 1
AND date_ between date_trunc('week',current_date)- interval '7' day and date_trunc('week',current_date) - interval '1' day
AND date_format(a.date_,'%a') = 'Sun'
GROUP BY 1,2
)
,metrics AS 
(SELECT
       YEAR(a.date_)*100 + WEEK(a.date_) AS created_year_week 
      ,a.uid as shipper_id
      ,a.shipper_name 
      ,a.city_name
      ,a.hub_type_original AS hub_type
      ,cqs.is_qualified AS is_qualified_sun
      ,COUNT(a.slot_id) AS registered_slot
      ,SUM(a.final_kpi) AS total_kpi 
      ,COUNT(CASE WHEN total_order > 0 THEN a.slot_id ELSE NULL END) AS working_slot        

   
 
FROM 
(SELECT a.*
       ,CASE WHEN p.uid is not null then 1 
             ELSE a.kpi END AS final_kpi    
FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics a 
LEFT JOIN params p 
    on p.uid = a.uid 
    and p.report_date = a.date_
) a 



LEFT JOIN check_qualified_sun cqs
    on cqs.created_year_week = (YEAR(a.date_)*100 + WEEK(a.date_))    
    and cqs.shipper_id = a.uid
    
WHERE 1 = 1                                            
AND a.date_ between date_trunc('week',current_date)- interval '7' day and date_trunc('week',current_date) - interval '1' day
AND date_format(a.date_,'%a') = 'Sun'
AND a.registered_ = 1
AND a.city_name in ('HCM City','Ha Noi City')
GROUP BY 1,2,3,4,5,6
)
-- SELECT * FROM metrics WHERE shipper_id = 16597232

SELECT
         created_year_week
        ,shipper_id
        ,shipper_name 
        ,city_name
        ,hub_type
        ,is_qualified_sun
        ,total_kpi
        ,CASE 
             WHEN hub_type in ('8 hour shift','10 hour shift') AND is_qualified_sun = 1 AND total_kpi >= 1 then 50000
             WHEN hub_type = '5 hour shift' AND is_qualified_sun = 1 AND total_kpi >= 2 then 50000
             WHEN hub_type = '5 hour shift' AND is_qualified_sun = 1 AND total_kpi >= 1 then 30000
             WHEN hub_type = '3 hour shift' AND is_qualified_sun = 1 AND total_kpi >= 2 then 30000
             WHEN hub_type = '3 hour shift' AND is_qualified_sun = 1 AND total_kpi >= 1 then 20000
             ELSE 0 END AS sunday_bonus


FROM metrics                

