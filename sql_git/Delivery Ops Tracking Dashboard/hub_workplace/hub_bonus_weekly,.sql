WITH params(report_date,uid) AS
(VALUES
(date'2022-02-02',1) 
) 
,check_qualified AS
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
GROUP BY 1,2
)
,metrics AS 
(SELECT
       YEAR(a.date_)*100 + WEEK(a.date_) AS created_year_week 
      ,a.uid as shipper_id
      ,a.shipper_name 
      ,a.city_name
      ,a.hub_type_original AS hub_type
      ,cq.is_qualified  
      ,SUM(a.registered_) AS total_reg
      ,COUNT(DISTINCT CASE WHEN total_order > 0 then (a.date_,a.slot_id) ELSE NULL END) AS working_day  
      ,SUM(final_kpi) AS total_kpi
      ,SUM(CASE WHEN date_format(a.date_,'%a') = 'Sun' THEN a.kpi ELSE 0 END) AS kpi_sun
      ,SUM(case when date_format(a.date_,'%a') = 'Sun' then a.total_order else 0 end) as is_work_sun         
FROM 
(SELECT a.*
       ,CASE 
             WHEN hub_type_original = '3 hour shift' then 0
             WHEN p.uid is not null then 1
             ELSE a.kpi END AS final_kpi    
FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics a 
LEFT JOIN params p 
    on p.uid = a.uid 
    and p.report_date = a.date_                
)a

LEFT JOIN check_qualified cq 
    on cq.created_year_week = (YEAR(a.date_)*100 + WEEK(a.date_))
    and cq.shipper_id = a.uid

WHERE 1 = 1                                            
AND date_ between date_trunc('week',current_date)- interval '7' day and date_trunc('week',current_date) - interval '1' day
AND registered_ = 1 
GROUP BY 1,2,3,4,5,6
)
SELECT 
       created_year_week
      ,shipper_id
      ,shipper_name
      ,city_name
      ,hub_type
      ,CASE
            WHEN hub_type in ('8 hour shift','10 hour shift') AND city_name in ('HCM City','Ha Noi City') AND total_kpi >= 6 AND is_qualified = 1 THEN 300000
            WHEN hub_type in ('8 hour shift','10 hour shift') AND city_name in ('HCM City','Ha Noi City') AND total_kpi >= 5 AND is_qualified = 1 THEN 150000
            WHEN hub_type in ('8 hour shift','10 hour shift') AND city_name in ('HCM City','Ha Noi City') AND total_kpi >= 4 AND is_qualified = 1 THEN 100000
            WHEN hub_type in ('8 hour shift','10 hour shift') AND city_name = 'Hai Phong City' AND total_kpi >= 6 AND is_qualified = 1 THEN 200000            
            WHEN hub_type in ('8 hour shift','10 hour shift') AND city_name = 'Hai Phong City' AND total_kpi >= 4 AND is_qualified = 1 THEN 80000

            WHEN hub_type = '5 hour shift' AND city_name in ('HCM City','Ha Noi City') AND total_kpi >= 12 AND is_qualified = 1 THEN 500000
            WHEN hub_type = '5 hour shift' AND city_name in ('HCM City','Ha Noi City') AND total_kpi >= 9 AND is_qualified = 1 THEN 300000
            WHEN hub_type = '5 hour shift' AND city_name in ('HCM City','Ha Noi City') AND total_kpi >= 6 AND is_qualified = 1 THEN 150000
            WHEN hub_type = '5 hour shift' AND city_name = 'Hai Phong City' AND total_kpi >= 9 AND is_qualified = 1 THEN 200000
            WHEN hub_type = '5 hour shift' AND city_name = 'Hai Phong City' AND total_kpi >= 6 AND is_qualified = 1 THEN 80000
            ELSE 0 END AS weekly_bonus
      ,total_reg
      ,working_day
      ,total_kpi
      ,is_qualified     


FROM metrics

WHERE 1 = 1

