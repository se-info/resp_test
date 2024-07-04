with params(hub_type,city_name,start_bonus,end_bonus,bonus_value,bonus_add_on) AS 
(VALUES 
('5 hour shift','Hai Phong City',10,12,500,0),
('5 hour shift','Hai Phong City',13,16,1000,1500),
('5 hour shift','Hai Phong City',17,9999,1500,5500),

('8 hour shift','Hai Phong City',10,12,500,0),
('8 hour shift','Hai Phong City',13,16,1000,1500),
('8 hour shift','Hai Phong City',17,9999,1500,5500),

('10 hour shift','Hai Phong City',10,12,500,0),
('10 hour shift','Hai Phong City',13,16,1000,1500),
('10 hour shift','Hai Phong City',17,9999,1500,5500),

('3 hour shift','HCM City',7,14,2000,0),
('3 hour shift','HCM City',15,9999,3000,16000),

('3 hour shift','Ha Noi City',7,14,2000,0),
('3 hour shift','Ha Noi City',15,9999,3000,16000),

('5 hour shift','HCM City',14,24,4000,0),
('5 hour shift','HCM City',25,9999,6000,44000),

('5 hour shift','Ha Noi City',14,24,4000,0),
('5 hour shift','Ha Noi City',25,9999,6000,44000),

('8 hour shift','HCM City',26,30,4000,0),
('8 hour shift','HCM City',31,9999,6000,20000),

('8 hour shift','Ha Noi City',26,30,4000,0),
('8 hour shift','Ha Noi City',31,9999,6000,20000),

('10 hour shift','HCM City',31,9999,6000,0),
('10 hour shift','Ha Noi City',31,9999,6000,0)
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
,raw AS 
(SELECT 
        raw.date_
       ,raw.uid AS shipper_id 
       ,raw.shipper_name 
       ,raw.city_name 
       ,raw.hub_type_original
       ,raw.hub_type_x_start_time
       ,raw.in_shift_online_time
       ,raw.online_peak_hour
       ,raw.is_auto_accept
       ,(raw.deny_count + raw.ignore_count) AS deny_ignore_cnt
       ,raw.kpi
       ,raw.kpi_failed
       ,raw.total_order
       ,raw.total_order*13500 AS ship_shared    
       ,raw.extra_ship
       ,raw.daily_bonus
       ,raw.total_income
       ,CASE WHEN raw.city_name in ('HCM City','Ha Noi City') and LOWER(TRIM(raw.hub_type_original)) in ('8 hour shift') THEN 25
           WHEN raw.city_name in ('HCM City','Ha Noi City') and LOWER(TRIM(raw.hub_type_original)) in ('10 hour shift') THEN 30
           ELSE 0 END AS minimum_threshold
        ,REGEXP_LIKE(raw.kpi_failed,'Auto Accept|Online in shift|Online peak hour')  AS condition_1    
        ,CASE 
          WHEN (deny_count + ignore_count) <= 1 THEN true ELSE false END AS condition_2
        ,CASE 
          WHEN raw.hub_type_original = '3 hour shift' AND total_order >= 6 THEN true
          WHEN raw.hub_type_original = '5 hour shift' AND total_order >= 13 THEN true
          WHEN raw.hub_type_original = '8 hour shift' AND total_order >= 24 THEN true
          WHEN raw.hub_type_original = '10 hour shift' AND total_order >= 29 THEN true
          ELSE false END AS condition_3  
        ,COALESCE(p.bonus_value,0) AS bonus_value
        ,COALESCE(p.start_bonus -1,0) AS start_bonus
        ,COALESCE(p.end_bonus,0) AS end_bonus
        ,COALESCE(p.bonus_add_on,0) AS bonus_add_on
        ,cq.is_qualified AS weekly_bonus_qualified
        ,cqs.is_qualified AS sunday_bonus_qualified


FROM dev_vnfdbi_opsndrivers.phong_hub_driver_metrics raw 

LEFT JOIN check_qualified cq 
    on cq.created_year_week = (YEAR(raw.date_)*100 + WEEK(raw.date_))
    and cq.shipper_id = raw.uid 

LEFT JOIN check_qualified_sun cqs
    on cqs.created_year_week = (YEAR(raw.date_)*100 + WEEK(raw.date_))    
    and cqs.shipper_id = raw.uid

LEFT JOIN params p 
    on TRIM(p.hub_type) = TRIM(raw.hub_type_original)
    and p.city_name = raw.city_name
    and raw.total_order BETWEEN p.start_bonus and p.end_bonus

WHERE registered_ = 1 
)
,summary AS 
(SELECT 
      raw.date_
     ,shipper_id
     ,ship_shared
     ,daily_bonus
     ,extra_ship
     ,total_income as current_income
     ,raw.hub_type_x_start_time
     ,raw.kpi_failed
     ,raw.condition_1 
     ,raw.condition_2 
     ,raw.condition_3
     ,raw.kpi AS kpi_original
     ,raw.total_order
     ,CASE 
          WHEN raw.city_name in ('HCM City','Ha Noi City') 
            AND raw.hub_type_original in ('8 hour shift', '10 hour shift')  
            AND raw.total_order <= minimum_threshold
            AND raw.condition_1 = false 
            AND raw.condition_2 = true 
            AND raw.condition_3 = true
            AND raw.hub_type_x_start_time in ${hub_check} 
            AND raw.kpi = 0
            THEN 1
          WHEN  raw.start_bonus > 0 
            AND raw.condition_1 = false 
            AND raw.condition_2 = true 
            AND raw.condition_3 = true
            AND raw.hub_type_x_start_time in ${hub_check} 
            AND raw.kpi = 0
            THEN 1
          WHEN raw.kpi = 1 THEN 1  
        ELSE 0 END AS kpi_adjusted
     ,CASE 
          WHEN raw.city_name in ('HCM City','Ha Noi City') 
            AND raw.hub_type_original in ('8 hour shift', '10 hour shift')  
            AND raw.total_order <= minimum_threshold
            AND raw.condition_1 = false 
            AND raw.condition_2 = true 
            AND raw.condition_3 = true 
            AND raw.hub_type_x_start_time in ${hub_check} 
            AND raw.kpi = 0
            THEN (13500 * raw.minimum_threshold ) - ship_shared
          WHEN  raw.start_bonus > 0 
            AND raw.condition_1 = false 
            AND raw.condition_2 = true 
            AND raw.condition_3 = true
            AND raw.hub_type_x_start_time in ${hub_check} 
            AND raw.kpi = 0
            THEN ((raw.total_order - raw.start_bonus) *raw.bonus_value) + bonus_add_on
        ELSE 0 END AS compensated_value                  
     ,CASE 
          WHEN raw.city_name in ('HCM City','Ha Noi City') 
            AND raw.hub_type_original in ('8 hour shift', '10 hour shift') 
            AND raw.total_order <= minimum_threshold
            AND raw.condition_1 = false 
            AND raw.condition_2 = true 
            AND raw.condition_3 = true
            AND raw.hub_type_x_start_time in ${hub_check} 
            AND raw.kpi = 0
            THEN 13500 * raw.minimum_threshold    
          WHEN  raw.start_bonus > 0 
            AND raw.condition_1 = false 
            AND raw.condition_2 = true 
            AND raw.condition_3 = true
            AND raw.hub_type_x_start_time in ${hub_check} 
            AND raw.kpi = 0
            THEN ((raw.total_order - raw.start_bonus) *raw.bonus_value) + ship_shared + raw.bonus_add_on
        ELSE 0 END AS after_compensated

FROM raw



WHERE 1 = 1
)

SELECT * FROM summary where shipper_id = ${input_id}