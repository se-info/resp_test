SELECT
        oct.id AS order_id,
        oct.order_code,
        -- oct.city_id,
        city.name_en AS city_name,
        oct.shipper_uid AS shipper_id,
        CASE 
        WHEN CAST(JSON_EXTRACT(oct.extra_data,'$.risk_bearer_type') AS INT) = 1 THEN 'Now' 
        ELSE 'Driver' END AS risk_bearer,
        CAST(JSON_EXTRACT(oct.extra_data,'$.admin_note') AS VARCHAR) AS agent_note,
        COALESCE(CAST(JSON_EXTRACT(bo.note_content,'$.default') AS VARCHAR),bo.extra_note) AS  quit_reason,
        CAST(JSON_EXTRACT(oct.extra_data,'$.is_quit_refund') AS VARCHAR) AS is_quit_refund,
        merchant_paid_amount*1.00/100 AS merchant_paid_amount,
        sub_total*1.00/100 AS sub_total,
        total_amount*1.00/100 AS total_amount,
        total_shipping_fee*1.00/100 AS user_ship_fee,
        dot.delivery_cost*1.00/100 AS driver_ship_fee



FROM (select * from shopeefood.shopeefood_mart_dwd_vn_order_completed_da where date(dt) = current_date - interval '1' day) oct 

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day and ref_order_category = 0 ) dot 
    on dot.ref_order_id = oct.id

LEFT JOIN shopeefood.foody_mart__fact_order_note bo 
    ON bo.order_id = oct.id
    AND bo.note_type_id = 3    

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
    on city.id = oct.city_id 
    and city.country_id = 86


WHERE 1 = 1 
AND oct.status = 9
-- AND oct.id = 619941085
AND DATE(FROM_UNIXTIME(oct.submit_time - 3600)) BETWEEN current_date - interval '30' day AND current_date - interval '1' day 
