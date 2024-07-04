with driver_order as
(SELECT        
        DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date
        ,dot.uid 
        ,COUNT(DISTINCT dot.ref_order_code) AS total_order    

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet 
    on dot.id = dotet.order_id
WHERE 1 = 1 
AND dot.order_status = 400
GROUP BY 1,2
)
SELECT 
      do.report_date
     ,do.uid AS shipper_id
     ,sm.shipper_name
     ,sm.city_name
     ,do.total_order
     ,(rp.completed_rate/CAST(100 AS DOUBLE)) AS sla_rate
     ,CASE 
          WHEN total_order BETWEEN 10 AND 15  AND (rp.completed_rate/CAST(100 AS DOUBLE)) >= 95 THEN 30000
          WHEN total_order >= 16 AND (rp.completed_rate/CAST(100 AS DOUBLE)) >= 95 THEN 60000
          ELSE 0 END AS bonus   


FROM driver_order do 

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
     on sm.shipper_id = do.uid 
     and TRY_CAST(sm.grass_date AS DATE) = do.report_date

LEFT JOIN shopeefood.foody_internal_db__shipper_report_daily_tab__reg_daily_s0_live rp 
     on do.uid =  rp.uid 
     and do.report_date = DATE(FROM_UNIXTIME(rp.report_date - 3600))


-- WHERE do.report_date BETWEEN DATE'2023-05-02' AND DATE'2023-05-04'
WHERE do.report_date = DATE'2023-05-05'      
AND (CASE 
          WHEN total_order BETWEEN 10 AND 15  AND (rp.completed_rate/CAST(100 AS DOUBLE)) >= 95 THEN 30000
          WHEN total_order >= 16 AND (rp.completed_rate/CAST(100 AS DOUBLE)) >= 95 THEN 60000
          ELSE 0 END) > 0 
      