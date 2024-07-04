WITH raw AS
(SELECT 
         order_code
        ,order_id 
        ,partner_id
        ,partner_run_type
        ,status_id
        ,CAST(json_extract(extra_data,'$.order.hub_shipping_fee') AS BIGINT) AS hub_order_fee
        ,from_unixtime(create_time - 3600) as create_timestamp
        -- ,extra_data
        ,CAST(JSON_EXTRACT(extra_data,'$.order.partner_order_shipping_fee.shipping_fee') AS BIGINT) AS ship_shared 
        ,CAST(JSON_EXTRACT(extra_data,'$.order.partner_order_shipping_fee.shipping_distance') AS DOUBLE) AS shipping_distance
        ,CAST(JSON_EXTRACT(extra_data,'$.order.parking_fee') AS BIGINT) AS parking_fee
        ,CAST(JSON_EXTRACT(extra_data,'$.order.merchant_parking_fee') AS BIGINT) AS mex_parking_fee
        ,CAST(JSON_EXTRACT(extra_data,'$.order.shipper_tip_fee') AS BIGINT) AS tip_fee
        ,CAST(JSON_EXTRACT(extra_data,'$.order.weather_fee_collected') AS BIGINT) AS bad_weather_fee
        ,CAST(JSON_EXTRACT(extra_data,'$.order.late_night_fee') AS BIGINT) AS late_night_fee



FROM 
(  --- data current
select *

from shopeefood.foody_accountant_db__order_delivery_tab__reg_daily_s0_live

UNION ALL 
--- data from -2021
select *
from shopeefood.foody_accountant_archive_db__order_delivery_tab__reg_daily_s0_live
) ad_odt

UNION ALL 
SELECT
        order_code
        ,order_id 
        ,partner_id
        ,partner_run_type
        ,status_id
        ,CAST(json_extract(extra_data,'$.order.hub_shipping_fee') AS BIGINT) AS hub_order_fee
        ,from_unixtime(create_time - 3600) as create_timestamp
        -- ,extra_data
        ,CAST(JSON_EXTRACT(extra_data,'$.order.partner_order_shipping_fee.shipping_fee') AS BIGINT) AS ship_shared 
        ,CAST(JSON_EXTRACT(extra_data,'$.order.partner_order_shipping_fee.shipping_distance') AS DOUBLE) AS shipping_distance
        ,CAST(JSON_EXTRACT(extra_data,'$.order.parking_fee') AS BIGINT) AS parking_fee
        ,CAST(JSON_EXTRACT(extra_data,'$.order.merchant_parking_fee') AS BIGINT) AS mex_parking_fee
        ,CAST(JSON_EXTRACT(extra_data,'$.order.shipper_tip_fee') AS BIGINT) AS tip_fee
        ,CAST(JSON_EXTRACT(extra_data,'$.order.weather_fee_collected') AS BIGINT) AS bad_weather_fee
        ,CAST(JSON_EXTRACT(extra_data,'$.order.late_night_fee') AS BIGINT) AS late_night_fee

from 
(select * from shopeefood.foody_accountant_db__order_now_ship_shopee_tab__vn_daily_s0_live
-- UNION ALL 
-- select * from shopeefood.foody_accountant_db__order_now_ship_merchant_tab__reg_daily_s0_live
-- UNION ALL 
-- select * from shopeefood.foody_accountant_db__order_now_ship_user_tab__reg_daily_s0_live
)

)
select * from raw

WHERE 1 = 1 
AND order_id in (542489723,542490448)
-- AND date(create_timestamp) = current_date - interval '1' day