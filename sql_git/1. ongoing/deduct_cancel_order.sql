SELECT  
        id AS order_id,
        order_code,
        restaurant_id AS merchant_id,
        CAST(JSON_EXTRACT(extra_data,'$.restaurant_name') AS VARCHAR) AS merchant_name,
        shipper_uid AS shipper_id,
        sub_total*1.0000/100 AS gmv_foody,
        commission_amount*1.0000/100 AS coms_fdy,
        merchant_discount_amount*1.0000/100 AS merchant_discount,
        total_discount_amount*1.0000/100 AS foody_discount,
        -- total_restaurant_discount,
        0 AS coin_earned,
        CAST(JSON_EXTRACT(extra_data,'$.platform_service_fee') AS BIGINT) AS platform_fee,
        CAST(JSON_EXTRACT(extra_data,'$.late_night_service_fee') AS BIGINT) AS late_night_fee,
        CAST(JSON_EXTRACT(extra_data,'$.bad_weather_fee.user_pay_amount') AS BIGINT) AS bad_weather_fee,
        CAST(JSON_EXTRACT(extra_data,'$.small_order_fee') AS BIGINT) AS late_night_fee,
        total_shipping_fee*1.0000/100 AS user_ship_fee,
        dot.delivery_cost*1.0000/100 AS driver_ship_fee,
        shipper_bonus_amount*1.0000/100 AS bonus_shipper,
        DATE(FROM_UNIXTIME(final_delivered_time - 3600)) AS delivered_date

FROM shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live raw 

LEFT JOIN (SELECT ref_order_id,delivery_cost FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da 
                    WHERE date(dt) = current_date - interval '1' day
                    AND ref_order_category = 0 ) dot 
    on dot.ref_order_id = raw.id

;
-- Check promotion code 
select * from shopeefood.foody_mart__fact_order_promotion where order_id = 821971186

