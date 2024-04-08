WITH report_date_time AS 
(SELECT
        DATE(report_date) AS report_date
        ,sequence(cast(t.report_date as timestamp ) ,cast(t.report_date as timestamp) + interval '86400' second, interval '600' second  ) dt_array 
        ,1 as mapping
FROM
(
(
SELECT sequence(current_date - interval '7' day, current_date - interval '1' day) bar 
)
CROSS JOIN UNNEST (bar) as t(report_date)
)
)
,date_time AS 
(SELECT 
         t1.mapping
        ,t2.dt_array_unnest as start_time 
        ,t2.dt_array_unnest + interval '599.99' second as end_time 

FROM report_date_time t1 
CROSS JOIN UNNEST (dt_array) as t2(dt_array_unnest) 
)

,raw AS 
(SELECT date(a.created_timestamp) AS created_date 
       ,HOUR(a.created_timestamp) AS created_hour 
       ,a.*
        ,case 
            when oct.status not in (8) then null
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Quán đã đóng cửa','Quán đóng cửa','Shop was closed','Shop is closed','Shop was no longer operating','Quán cúp diện','Driver reported Merchant closed','Tài xế báo Quán đóng cửa') then 'Shop closed'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('No driver','không có driver','Đơn hàng chưa có Tài xế nhận','No Drivers found','Không có tài xế nhận giao hàng','Lack of shipper') then 'No driver'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Out of stock of all order items','Out of Stock','Quan het mon','Hết tẩt cả món trong đơn hàng','Quán hết món','Merchant/Driver reported out of stock','Cửa hàng/ Tài xế báo hết món') then 'Out of stock'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Make another order again','Customer wanted to cancel order','Want to cancel') then 'Make another order'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) = 'I inputted the wrong information contact' then 'Customer put wrong contact info'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Shop was busy','Quán làm không kịp','Shop could not prepare in time','Cannot prepage') then 'Shop busy'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Cancelled_Napas','Cancelled_Credit Card','Cancelled_VNPay','Cancelled_vnpay','Cancelled_cybersource','Customer payment failed','Payment failed','Lỗi thanh toán','Payment is failed') then 'Payment failed'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) LIKE '%deliver on time%' then trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note')))
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Take too long to confirm order','Đơn hàng xác nhận quá lâu','Confirmed the order too late','Xác nhận đơn hàng chậm quá') then 'Confirmed the order too late'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Shop did not confirm order') then 'Shop did not confirm'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Too high item price/shipping fees','Giá món/ chi phí cao') then 'Think again on price and fees'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('I want to change item/Merchant','Tôi muốn đổi món/Quán') then 'Change item/Merchant'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('I want to change delivery time','Tôi muốn đổi giờ giao') then 'Change delivery time'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Tôi muốn đổi thông tin liên hệ','I want to change phone number') then 'Change phone number'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Tôi muốn đổi hình thức thanh toán','I want to change payment method') then 'Change payment method'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Tôi muốn đổi thông tin liên hệ','I want to change phone number') then 'Change phone number'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Tôi đặt trùng đơn','I made duplicate orders') then 'I made duplicate orders'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note')))in ('Tôi bận nên không thể nhận hàng','I am busy and cannot receive order','Có việc đột xuất') then 'I am busy and cannot receive order'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Ngân hàng chưa xác nhận thanh toán','Pending payment status from bank') then 'Pending status from bank'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Khách hàng chưa hoàn thành thanh toán','Incomplete payment process','User closed payment page')then 'Incomplete payment'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Tôi quên nhập Mã Code khuyến mãi','I forgot inputting the discount code','I forgot inputting discount code') then 'Forgot inputting discount code'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Wrong price','Sai giá món') then 'Wrong price'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Affected by quarantine area','Ảnh hưởng do khu vực cách ly') then 'Affected by quarantine area'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Merchant cannot accept the order at the moment','Hiện tại Quán không thể tiếp nhận thêm đơn') then 'Order limit due to Covid'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) = '' then 'Others'
            when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) is null then 'Others'
            else 'Others' end as cancel_reason        

FROM dev_vnfdbi_opsndrivers.phong_raw_order_checking a 

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_vn_order_completed_da where date(dt) = current_date - interval '1' day ) oct
    on oct.order_code = a.ref_order_code

LEFT JOIN shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr 
    on cr.id = try_cast(json_extract_scalar(oct.extra_data,'$.note_ids') as int)

WHERE source = 'order_delivery'
)
,pm_raw as 
(SELECT 
         date(from_unixtime(a.start_time - 3600)) as created_date
        ,from_unixtime(a.start_time - 3600) as start_ts
        ,from_unixtime((a.start_time + running_time) - 3600) as end_ts
        ,running_time/cast(60 as double) as running_ts
        ,assigning_order
        ,available_driver 
        ,online_driver
        ,city_id
        ,district_id
        ,city.name_en AS city_name
        ,di.name_en AS district_name
        ,b.name as peak_mode_name

FROM shopeefood.foody_delivery_admin_db__peak_mode_export_activity_tab__reg_daily_s0_live a

LEFT JOIN shopeefood.foody_delivery_admin_db__peak_mode_tab__reg_daily_s0_live b 
    on b.id = a.mode_id

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city 
    on city.id = a.city_id 
    and city.country_id = 86

LEFT JOIN shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di 
    on di.id = a.district_id

WHERE 1 = 1     
AND DATE(FROM_UNIXTIME(a.start_time - 3600)) BETWEEN current_date - interval '60' day AND current_date - interval '1' day
)
,metrics AS
(SELECT 
         raw.created_date
        ,raw.created_hour
        ,raw.created_timestamp
        ,raw.pick_city_name
        ,raw.pick_district_name
        ,CASE 
            WHEN pm.peak_mode_name in ('Peak 1 Mode','Peak 2 Mode','Peak 3 Mode') THEN pm.peak_mode_name
            ELSE 'Normal Mode' END AS peak_mode
        ,pm.assigning_order AS assigning_order_sum
        ,pm.available_driver AS available_driver_sum       
        ,raw.ref_order_code     
        ,raw.order_status
        ,raw.cancel_reason
        ,1 AS mapping

FROM raw 

left join pm_raw pm 
    on pm.city_name = raw.pick_city_name 
    and pm.district_name = raw.pick_district_name
    and raw.created_timestamp >= pm.start_ts 
    and raw.created_timestamp < pm.end_ts


-- WHERE (raw.created_date = date'2023-02-02' or raw.created_date = date'2023-03-08')
)
,final_metrics AS
(SELECT  
         dt.* 
        ,m.ref_order_code
        ,m.peak_mode
        ,m.order_status
        ,m.pick_city_name
        ,m.pick_district_name
        ,m.cancel_reason
        ,CASE 
              WHEN m.created_timestamp >= dt.start_time AND m.created_timestamp <= dt.end_time THEN 1 
              ELSE 0 END AS is_valid

FROM  date_time dt 
LEFT JOIN metrics m
    on dt.mapping = m.mapping

WHERE 1 = 1 
AND (CASE 
         WHEN m.created_timestamp >= dt.start_time AND m.created_timestamp <= dt.end_time THEN 1 
         ELSE 0 END ) = 1
)
SELECT
        DATE(start_time) AS created_date
       ,start_time
       ,end_time
       ,pick_city_name
       ,pick_district_name
       ,COUNT(DISTINCT ref_order_code) AS gross_order
       ,COUNT(DISTINCT CASE WHEN order_status = 'DELIVERED' THEN ref_order_code ELSE NULL END) net_order
       ,COUNT(DISTINCT CASE WHEN order_status IN ('SYSTEM_CANCELLED','USER_CANCELLED','EXTERNAL_CANCELLED') AND cancel_reason = 'No driver' THEN ref_order_code ELSE NULL END) cnd_order
       ,COUNT(DISTINCT CASE WHEN peak_mode = 'Normal Mode' THEN ref_order_code ELSE NULL END) AS normal_order
       ,COUNT(DISTINCT CASE WHEN peak_mode = 'Peak 1 Mode' THEN ref_order_code ELSE NULL END) AS peak1_order
       ,COUNT(DISTINCT CASE WHEN peak_mode = 'Peak 2 Mode' THEN ref_order_code ELSE NULL END) AS peak2_order
       ,COUNT(DISTINCT CASE WHEN peak_mode = 'Peak 3 Mode' THEN ref_order_code ELSE NULL END) AS peak3_order

FROM final_metrics


WHERE 1 = 1 
AND pick_city_name = 'HCM City'
AND DATE(start_time) = current_date - interval '1' day
GROUP BY 1,2,3,4,5
ORDER BY 1,2 ASC

