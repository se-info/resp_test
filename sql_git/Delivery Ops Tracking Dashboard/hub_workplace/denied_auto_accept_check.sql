WITH raw AS
(SELECT 
        sa.created_date
       ,sa.shipper_id
       ,hm.hub_type_original
       ,hm.hub_type_x_start_time
       ,hm.registration_status
       ,sm.city_name AS shipper_city_name
       ,hm.extra_ship
       ,hm.daily_bonus
       ,hm.is_auto_accept
       ,hm.total_order AS total_delivered
       ,hm.kpi
       ,COUNT(DISTINCT CASE WHEN metrics != 'Denied' THEN (sa.order_id,sa.shipper_id) ELSE NULL END) AS total_assign
       ,COUNT(DISTINCT CASE WHEN metrics = 'Denied' THEN (sa.order_id,sa.shipper_id) ELSE NULL END) AS total_denied
       ,COUNT(DISTINCT CASE WHEN metrics = 'Ignored' THEN (sa.order_id,sa.shipper_id) ELSE NULL END) AS total_ignore
       ,COUNT(DISTINCT CASE WHEN metrics = 'Denied' AND is_denied_aa = 1 THEN (sa.order_id,sa.shipper_id) ELSE NULL END) AS total_denied_auto_accept


        
FROM dev_vnfdbi_opsndrivers.phong_raw_assignment_test sa

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = sa.shipper_id 
    and try_cast(sm.grass_date as date) = sa.created_date

LEFT JOIN dev_vnfdbi_opsndrivers.phong_hub_driver_metrics hm 
    on hm.uid = sa.shipper_id
    and sa.created_timestamp between hm.start_shift_time and hm.end_shift_time
    -- and hm.registered_ = 1 

WHERE 1 = 1 
AND sm.shipper_type_id = 12    

GROUP BY 1,2,3,4,5,6,7,8,9,10,11
ORDER BY sa.created_date DESC
)
SELECT 
        *
        ,CASE WHEN kpi = 1 and (extra_ship > 0 or daily_bonus > 0) AND total_denied_auto_accept >= 1 THEN 1 ELSE 0 END AS is_fraud

FROM raw
WHERE hub_type_original IS NOT NULL 
LIMIT 10 