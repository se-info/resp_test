WITH raw AS 
(SELECT 
         DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date
        ,dot.uid AS shipper_id  
        ,sm.shipper_name
        ,sm.city_name
        ,COUNT(DISTINCT dot.ref_order_code) AS total_order




FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet 
    ON dot.id = dotet.order_id
 
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = dot.uid 
    and TRY_CAST(sm.grass_date AS DATE) = DATE(FROM_UNIXTIME(dot.submitted_time - 3600))

WHERE 1 = 1 
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN DATE'${start_date}' AND DATE'${end_date}'
AND dot.order_status = 400
AND sm.city_name = '${city_name}'
GROUP BY 1,2,3,4
)
,summary AS 
(SELECT 
         shipper_id
        ,shipper_name
        ,city_name
        ,COUNT(DISTINCT report_date) AS working_day
        ,SUM(total_order) AS total_order
        
        
FROM raw    

GROUP BY 1,2,3
)

SELECT 
        r.* 
       ,CASE 
            WHEN s.working_day >= 3 AND  r.total_order BETWEEN 12 AND 19 THEN 40000
            WHEN s.working_day >= 3 AND  r.total_order >= 20 THEN 90000
            ELSE 0 END AS bonus_value


FROM raw r 

LEFT JOIN summary s 
    on s.shipper_id = r.shipper_id 
