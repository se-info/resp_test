with factor(hub_type,minimum_threshold,bonus_threshold,city_name) as 
(VALUES 
('3 hour shift',0,7,'HCM City'),
('3 hour shift',0,7,'Ha Noi City'),

('5 hour shift',0,14,'HCM City'),
('5 hour shift',0,14,'Ha Noi City'),
('5 hour shift',0,10,'Hai Phong City'),

('8 hour shift',25,26,'HCM City'),
('8 hour shift',25,26,'Ha Noi City'),
('8 hour shift',0,10,'Hai Phong City'),

('10 hour shift',30,31,'HCM City'),
('10 hour shift',30,31,'Ha Noi City'),
('10 hour shift',0,10,'Hai Phong City')
)
,metrics as 
(SELECT         dot.uid AS shipper_id
              ,rp.city_name  
              ,TRIM(rp.hub_type) AS hub_type 
              ,CASE WHEN dot.is_asap = 1 then DATE(FROM_UNIXTIME(dot.submitted_time - 3600)) ELSE DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) END AS report_date  
              ,CASE WHEN dot.is_asap = 1 then HOUR(FROM_UNIXTIME(dot.submitted_time - 3600)) ELSE HOUR(FROM_UNIXTIME(dot.real_drop_time - 3600)) END AS report_hour
              ,rp.kpi AS is_qualified_kpi               
              ,COUNT(DISTINCT dot.ref_order_id) AS total_ado

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet 
    on dot.id = dotet.order_id

LEFT JOIN dev_vnfdbi_opsndrivers.phong_hub_driver_metrics rp on rp.uid = dot.uid and rp.date_ = DATE(FROM_UNIXTIME(dot.real_drop_time - 3600))


WHERE 1 = 1 
AND dot.order_status = 400
AND CAST(json_extract(dotet.order_data,'$.shipper_policy.type') AS BIGINT ) = 2 
AND (CASE WHEN dot.is_asap = 1 then DATE(FROM_UNIXTIME(dot.submitted_time - 3600)) 
          ELSE DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) END ) BETWEEN current_date - interval '15' day and current_date - interval '2' day


GROUP BY 1,2,3,4,5,6
)
,final as 
(SELECT 
        m.*
       ,f.minimum_threshold
       ,f.bonus_threshold           
       ,SUM(total_ado)OVER(partition by m.shipper_id,m.report_date order by report_hour) as sum_accumulate

FROM metrics m 

LEFT JOIN factor f on TRIM(f.hub_type) = TRIM(m.hub_type) and f.city_name = m.city_name



GROUP BY 1,2,3,4,5,6,7,8,9
)
SELECT 
        report_date
       ,report_hour
       ,hub_type
       ,city_name
       ,factor_range
       ,COUNT(DISTINCT shipper_id) as total_drivers 
       ,SUM(total_ado) as total_orders  
FROM
(SELECT 
        f.* 
       ,CASE 
            WHEN hub_type in ('3 hour shift','5 hour shift')  and is_qualified_kpi = 1 and sum_accumulate < bonus_threshold then 1
            WHEN hub_type in ('3 hour shift','5 hour shift')  and is_qualified_kpi = 1 and sum_accumulate >= bonus_threshold then 2
            ---
            WHEN hub_type in ('3 hour shift','5 hour shift')  and is_qualified_kpi = 0 and sum_accumulate < bonus_threshold then 3
            WHEN hub_type in ('3 hour shift','5 hour shift')  and is_qualified_kpi = 0 and sum_accumulate >= bonus_threshold then 4
            ---
            WHEN hub_type in ('8 hour shift','10 hour shift')  and is_qualified_kpi = 1 and sum_accumulate < minimum_threshold then 5
            WHEN hub_type in ('8 hour shift','10 hour shift')  and is_qualified_kpi = 1 and minimum_threshold <= sum_accumulate and sum_accumulate < bonus_threshold then 6
            WHEN hub_type in ('8 hour shift','10 hour shift')  and is_qualified_kpi = 1 and sum_accumulate >= bonus_threshold then 7
            ---
            WHEN hub_type in ('8 hour shift','10 hour shift')  and is_qualified_kpi = 0 and sum_accumulate < minimum_threshold then 8
            WHEN hub_type in ('8 hour shift','10 hour shift')  and is_qualified_kpi = 0 and minimum_threshold <= sum_accumulate and sum_accumulate < bonus_threshold then 9
            WHEN hub_type in ('8 hour shift','10 hour shift')  and is_qualified_kpi = 0 and sum_accumulate >= bonus_threshold then 10
            END AS factor_range 

            


FROM final f)
WHERE hub_type is not null
AND is_qualified_kpi is not null
GROUP BY 1,2,3,4,5