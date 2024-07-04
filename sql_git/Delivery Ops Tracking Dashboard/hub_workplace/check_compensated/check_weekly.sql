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
AND YEAR(a.date_)*100 + WEEK(a.date_) = ${week_check}  
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
AND YEAR(a.date_)*100 + WEEK(a.date_) = ${week_check}  
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
        ,COALESCE(p.start_bonus - 1,0) AS start_bonus
        ,COALESCE(p.end_bonus,0) AS end_bonus
        ,cq.is_qualified AS weekly_bonus_qualified
        ,cqs.is_qualified AS sunday_bonus_qualified
        ,raw.slot_id


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
     ,raw.hub_type_original
     ,raw.kpi_failed
     ,raw.condition_1 
     ,raw.condition_2 
     ,raw.condition_3
     ,raw.kpi AS kpi_original
     ,raw.total_order
     ,raw.slot_id
     ,CASE 
          WHEN raw.city_name in ('HCM City','Ha Noi City') 
            AND raw.hub_type_original in ('8 hour shift', '10 hour shift')  
            AND raw.total_order <= minimum_threshold
            AND raw.condition_1 = false 
            AND raw.condition_2 = true 
            AND raw.condition_3 = true 
            AND CAST(raw.date_ AS VARCHAR) in ${date_adjusted}
            AND TRIM(raw.hub_type_x_start_time) in ${hub_shift_start_time_check}
            AND raw.kpi = 0
            THEN 1
          WHEN  raw.condition_1 = false 
            AND raw.condition_2 = true 
            AND raw.condition_3 = true
            AND CAST(raw.date_ AS VARCHAR) in ${date_adjusted}
            AND TRIM(raw.hub_type_x_start_time) in ${hub_shift_start_time_check}
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
            AND CAST(raw.date_ AS VARCHAR) in ${date_adjusted}
            AND TRIM(raw.hub_type_x_start_time) in ${hub_shift_start_time_check}
            AND raw.kpi = 0
            THEN (13500 * raw.minimum_threshold ) - ship_shared
          WHEN  raw.start_bonus > 0 
            AND raw.condition_1 = false 
            AND raw.condition_2 = true 
            AND raw.condition_3 = true
            AND CAST(raw.date_ AS VARCHAR) in ${date_adjusted}
            AND TRIM(raw.hub_type_x_start_time) in ${hub_shift_start_time_check}
            AND raw.kpi = 0
            THEN (raw.total_order - raw.start_bonus) *raw.bonus_value
        ELSE 0 END AS compensated_value                  
     ,CASE 
          WHEN raw.city_name in ('HCM City','Ha Noi City') 
            AND raw.hub_type_original in ('8 hour shift', '10 hour shift') 
            AND raw.total_order <= minimum_threshold
            AND raw.condition_1 = false 
            AND raw.condition_2 = true 
            AND raw.condition_3 = true
            AND TRIM(raw.hub_type_x_start_time) in ${hub_shift_start_time_check}
            AND CAST(raw.date_ AS VARCHAR) in ${date_adjusted}
            AND raw.kpi = 0
            THEN 13500 * raw.minimum_threshold    
          WHEN  raw.start_bonus > 0 
            AND raw.condition_1 = false 
            AND raw.condition_2 = true 
            AND raw.condition_3 = true
            AND TRIM(raw.hub_type_x_start_time) in ${hub_shift_start_time_check}
            AND CAST(raw.date_ AS VARCHAR) in ${date_adjusted}
            AND raw.kpi = 0
            THEN ((raw.total_order - raw.start_bonus) *raw.bonus_value) + ship_shared
        ELSE 0 END AS after_compensated
     ,raw.weekly_bonus_qualified
     ,raw.sunday_bonus_qualified

FROM raw



WHERE 1 = 1
)
,check_paid AS 
(SELECT      
                  user_id
                --  ,note
                 ,SUM(CASE WHEN txn_type = 505 THEN balance/cast(100 as double) ELSE NULL END) as weekly_paid   
                 ,SUM(CASE WHEN txn_type = 520 THEN balance/cast(100 as double) ELSE NULL END) as sunday_paid   



FROM shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live

WHERE 1 = 1                                          
AND (note = 'HUB_MODEL_Thuong tai xe guong mau tuan '||${date_range_week}
     OR 
     note =  'HUB_MODEL_Thuong tai xe guong mau chu nhat tuan '||${date_range_sunday}
     )     
GROUP BY 1
)
,final AS
(SELECT  
         YEAR(date_)*100 + WEEK(date_) AS created_year_week
        ,shipper_id
        ,weekly_bonus_qualified
        ,sunday_bonus_qualified
        ,hub_type_original AS hub_type 
        ,ARRAY_AGG(DISTINCT hub_type_x_start_time) AS shift_ext_info
        ,MAP_AGG(date_,kpi_original) AS date_x_kpi_original_ext
        ,MAP_AGG(date_,kpi_adjusted) AS date_x_kpi_adjusted_ext

        ,COUNT(DISTINCT CASE WHEN total_order > 0 THEN slot_id ELSE NULL END) AS working_slot
        ,COUNT(DISTINCT CASE WHEN total_order > 0 AND kpi_original = 1 THEN slot_id ELSE NULL END) AS eligible_slot_orginal
        ,COUNT(DISTINCT CASE WHEN total_order > 0 AND kpi_adjusted = 1 THEN slot_id ELSE NULL END) AS eligible_slot_adjusted

        ,COUNT(DISTINCT CASE WHEN total_order > 0 AND kpi_original = 1 AND DATE_FORMAT(date_,'%a') = 'Sun' THEN slot_id ELSE NULL END) AS sunday_eligible_slot_orginal
        ,COUNT(DISTINCT CASE WHEN total_order > 0 AND kpi_adjusted = 1 AND DATE_FORMAT(date_,'%a') = 'Sun' THEN slot_id ELSE NULL END) AS sunday_eligible_slot_adjusted

        ,COALESCE(cp.weekly_paid,0) AS weekly_paid
        ,COALESCE(cp.sunday_paid,0) AS sunday_paid

FROM summary s 

LEFT JOIN check_paid cp 
    on cp.user_id = s.shipper_id

WHERE shipper_id = ${input_shipper_id}    

GROUP BY 1,2,3,4,5,cp.weekly_paid,cp.sunday_paid
)
,sum AS 
(SELECT 
         f.created_year_week
        ,f.shipper_id
        ,sm.shipper_name 
        ,sm.city_name
        ,CASE
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_orginal >= 6 AND f.weekly_bonus_qualified = 1 THEN 300000
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_orginal >= 5 AND f.weekly_bonus_qualified = 1 THEN 150000
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_orginal >= 4 AND f.weekly_bonus_qualified = 1 THEN 100000
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND sm.city_name = 'Hai Phong City' AND f.eligible_slot_orginal >= 6 AND f.weekly_bonus_qualified = 1 THEN 200000            
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND sm.city_name = 'Hai Phong City' AND f.eligible_slot_orginal >= 4 AND f.weekly_bonus_qualified = 1 THEN 80000

            WHEN f.hub_type = '5 hour shift' AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_orginal >= 12 AND f.weekly_bonus_qualified = 1 THEN 500000
            WHEN f.hub_type = '5 hour shift' AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_orginal >= 9 AND f.weekly_bonus_qualified = 1 THEN 300000
            WHEN f.hub_type = '5 hour shift' AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_orginal >= 6 AND f.weekly_bonus_qualified = 1 THEN 150000
            WHEN f.hub_type = '5 hour shift' AND sm.city_name = 'Hai Phong City' AND f.eligible_slot_orginal >= 9 AND f.weekly_bonus_qualified = 1 THEN 200000
            WHEN f.hub_type = '5 hour shift' AND sm.city_name = 'Hai Phong City' AND f.eligible_slot_orginal >= 6 AND f.weekly_bonus_qualified = 1 THEN 80000
            ELSE 0 END AS weekly_bonus_orginal

        ,CASE
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_adjusted >= 6 AND f.weekly_bonus_qualified = 1 THEN 300000
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_adjusted >= 5 AND f.weekly_bonus_qualified = 1 THEN 150000
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_adjusted >= 4 AND f.weekly_bonus_qualified = 1 THEN 100000
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND sm.city_name = 'Hai Phong City' AND f.eligible_slot_adjusted >= 6 AND f.weekly_bonus_qualified = 1 THEN 200000            
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND sm.city_name = 'Hai Phong City' AND f.eligible_slot_adjusted >= 4 AND f.weekly_bonus_qualified = 1 THEN 80000

            WHEN f.hub_type = '5 hour shift' AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_adjusted >= 12 AND f.weekly_bonus_qualified = 1 THEN 500000
            WHEN f.hub_type = '5 hour shift' AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_adjusted >= 9 AND f.weekly_bonus_qualified = 1 THEN 300000
            WHEN f.hub_type = '5 hour shift' AND sm.city_name in ('HCM City','Ha Noi City') AND f.eligible_slot_adjusted >= 6 AND f.weekly_bonus_qualified = 1 THEN 150000
            WHEN f.hub_type = '5 hour shift' AND sm.city_name = 'Hai Phong City' AND f.eligible_slot_adjusted >= 9 AND f.weekly_bonus_qualified = 1 THEN 200000
            WHEN f.hub_type = '5 hour shift' AND sm.city_name = 'Hai Phong City' AND f.eligible_slot_adjusted >= 6 AND f.weekly_bonus_qualified = 1 THEN 80000
            ELSE 0 END AS weekly_bonus_adjusted

        ,CASE 
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND f.sunday_bonus_qualified = 1 AND f.sunday_eligible_slot_orginal >= 1 then 50000
            WHEN f.hub_type = '5 hour shift' AND f.sunday_bonus_qualified = 1 AND f.sunday_eligible_slot_orginal >= 2 then 50000
            WHEN f.hub_type = '5 hour shift' AND f.sunday_bonus_qualified = 1 AND f.sunday_eligible_slot_orginal >= 1 then 30000
            WHEN f.hub_type = '3 hour shift' AND f.sunday_bonus_qualified = 1 AND f.sunday_eligible_slot_orginal >= 2 then 30000
            WHEN f.hub_type = '3 hour shift' AND f.sunday_bonus_qualified = 1 AND f.sunday_eligible_slot_orginal >= 1 then 20000
            ELSE 0 END AS sunday_bonus_original      

        ,CASE 
            WHEN f.hub_type in ('8 hour shift','10 hour shift') AND f.sunday_bonus_qualified = 1 AND f.sunday_eligible_slot_adjusted >= 1 then 50000
            WHEN f.hub_type = '5 hour shift' AND f.sunday_bonus_qualified = 1 AND f.sunday_eligible_slot_adjusted >= 2 then 50000
            WHEN f.hub_type = '5 hour shift' AND f.sunday_bonus_qualified = 1 AND f.sunday_eligible_slot_adjusted >= 1 then 30000
            WHEN f.hub_type = '3 hour shift' AND f.sunday_bonus_qualified = 1 AND f.sunday_eligible_slot_adjusted >= 2 then 30000
            WHEN f.hub_type = '3 hour shift' AND f.sunday_bonus_qualified = 1 AND f.sunday_eligible_slot_adjusted >= 1 then 20000
            ELSE 0 END AS sunday_bonus_adjusted
        ,weekly_paid
        ,sunday_paid
        ,eligible_slot_orginal
        ,eligible_slot_adjusted
        ,sunday_eligible_slot_orginal
        ,sunday_eligible_slot_adjusted
        ,hub_type


FROM final f 

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = f.shipper_id
    and sm.grass_date = 'current'

WHERE f.created_year_week = ${week_check}    
)

SELECT 
        created_year_week
       ,shipper_id
       ,shipper_name
       ,city_name
       ,SUM(weekly_bonus_orginal) AS weekly_bonus_orginal
       ,SUM(weekly_bonus_adjusted) AS weekly_bonus_adjusted
       ,SUM(sunday_bonus_original) AS sunday_bonus_original
       ,SUM(sunday_bonus_adjusted) AS sunday_bonus_adjusted
       ,MAX(weekly_paid) AS weekly_paid
       ,MAX(sunday_paid) AS sunday_paid
       ,SUM(eligible_slot_orginal) AS eligible_slot_orginal
       ,SUM(eligible_slot_adjusted) AS eligible_slot_adjusted
       ,SUM(sunday_eligible_slot_orginal) AS sunday_eligible_slot_orginal
       ,SUM(sunday_eligible_slot_adjusted) AS sunday_eligible_slot_adjusted
       ,ARRAY_JOIN(ARRAY_AGG(hub_type),',') AS hub_type_merge

FROM sum 

GROUP BY 1,2,3,4