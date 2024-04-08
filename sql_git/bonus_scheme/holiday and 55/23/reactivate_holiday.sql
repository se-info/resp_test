with driver_order as
(SELECT        
        DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date
        ,dot.uid 
        ,COUNT(DISTINCT dot.ref_order_code) AS total_order    

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet 
    on dot.id = dotet.order_id
WHERE 1 = 1 
-- AND dot.order_status = 400
AND dot.ref_order_status in (7,11)
GROUP BY 1,2
)
,check_wd AS 
(SELECT 
         do.uid
        ,COUNT(DISTINCT CASE WHEN do.total_order >= 6 THEN do.report_date ELSE NULL END) AS working_day  
        ,COUNT(DISTINCT CASE WHEN do.total_order >= 6 AND (completed_rate/CAST(100 AS DOUBLE)) >= 90 THEN do.report_date ELSE NULL END) AS working_day_qualified    

FROM driver_order do

LEFT JOIN shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp 
     on do.uid =  rp.uid 
     and do.report_date = DATE(FROM_UNIXTIME(rp.report_date - 3600))

WHERE do.report_date BETWEEN DATE'2023-05-02' AND DATE'2023-05-04'      
GROUP BY 1
)
SELECT 
      do.report_date
     ,do.uid AS shipper_id
     ,sm.shipper_name
     ,sm.city_name
     ,do.total_order
     ,(rp.completed_rate/CAST(100 AS DOUBLE)) AS sla_rate
     ,cw.working_day
     ,cw.working_day_qualified
     ,CASE 
          WHEN total_order BETWEEN 16 AND 29  AND (rp.completed_rate/CAST(100 AS DOUBLE)) >= 90 AND cw.working_day >= 3 THEN 30000
          WHEN total_order >= 30 AND (rp.completed_rate/CAST(100 AS DOUBLE)) >= 90 AND cw.working_day >= 3 THEN 70000
          ELSE 0 END AS bonus   


FROM driver_order do 

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
     on sm.shipper_id = do.uid 
     and TRY_CAST(sm.grass_date AS DATE) = do.report_date

LEFT JOIN shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp 
     on do.uid =  rp.uid 
     and do.report_date = DATE(FROM_UNIXTIME(rp.report_date - 3600))

LEFT JOIN check_wd cw 
     on cw.uid = do.uid  


WHERE do.report_date BETWEEN DATE'2023-05-02' AND DATE'2023-05-04'      