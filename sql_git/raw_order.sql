DROP TABLE IF EXISTS dev_vnfdbi_opsndrivers.phong_raw_order;
CREATE TABLE IF NOT EXISTS dev_vnfdbi_opsndrivers.phong_raw_order AS
WITH assignment AS 
(SELECT *

FROM dev_vnfdbi_opsndrivers.driver_ops_order_assign_log_tab
WHERE 1 = 1 
AND status in (3,4)
)    
,raw AS 
(SELECT 
         ns.id
        ,ns.uid
        ,ns.shipper_id
        ,ns.code as order_code
        ,ns.shopee_order_code
        ,ns.customer_id
        -- time
        ,from_unixtime(ns.create_time - 3600) as created_timestamp
        ,nsc.created_timestamp as canceled_timestamp
        ,DATE(from_unixtime(ns.create_time - 3600)) as created_date
        ,ns.distance*1.00/1000 as distance
        ,case when ns.booking_type = 2 and ns.booking_service_type = 1 then 'now_ship_user'
                when ns.booking_type = 3 and ns.booking_service_type = 1 then 'now_ship_merchant'
                when ns.booking_type = 4 and ns.booking_service_type = 1 then 'now_ship_shopee'
                when ns.booking_type = 2 and ns.booking_service_type = 2 then 'now_ship_same_day'
                when ns.booking_type = 2 and ns.booking_service_type = 3 then 'now_ship_multi_drop'
                else null end as source
        ,case when ns.booking_type = 2 and ns.booking_service_type = 1 then 4
                when ns.booking_type = 3 and ns.booking_service_type = 1 then 5
                when ns.booking_type = 4 and ns.booking_service_type = 1 then 6
                when ns.booking_type = 2 and ns.booking_service_type = 2 then 7
                when ns.booking_type = 2 and ns.booking_service_type = 3 then 8
                else null end as order_type

        -- order info
        ,case when ns.status = 11 then 'Delivered'
                when ns.status in (6,9,12) then 'Cancelled'
                when ns.booking_type = 4 and ns.status = 17 then 'Pickup Failed'
                when ns.booking_type <> 4 and ns.status = 23 then 'Pickup Failed'
                when ns.status = 14 then 'Returned'
                when ns.booking_type = 4 and ns.status = 3 then 'Assigning Timeout'
                else 'Others' end as order_status
        ,case when ns.status = 6 then 'User'
                when ns.status = 9 then 'Driver'
                when ns.status = 12 then 'System'
                else null end as cancel_by
        -- location
        ,case when ns.city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
        ,case when ns.city_id = 217 then 'HCM'
                when ns.city_id = 218 then 'HN'
                when ns.city_id = 219 then 'DN'
                ELSE 'OTH' end as city_group
        ,ns.city_id
        ,ns.district_id
        ,case when ns.pick_type = 1 then 1 else 0 end as is_asap
        ,case when ns.status in (6,9,12) then coalesce(nsc.cancel_reason,'No Reason') else null end as cancel_reason

        ,case when ns.status = 17 and ns.booking_type = 4 then pf_reason.pick_failed_reason else null end as pick_failed_reason

        ,date_diff('second',from_unixtime(ns.create_time - 3600),nsc.created_timestamp)*1.0000/60 as lt_submit_to_cancel
        ,case when ns.drop_real_time = 0 then null else date_diff('second',from_unixtime(ns.pick_real_time - 3600),from_unixtime(ns.drop_real_time - 3600))*1.0000/60 end as lt_deliver
        ,case when ns.status = 14 then date_diff('second',from_unixtime(ns.pick_real_time - 3600),from_unixtime(ns.update_time - 3600))*1.0000/60 else 0 end as lt_return

        ,case when ns.drop_real_time = 0 then NULL else from_unixtime(ns.drop_real_time - 3600) end as delivered_timestamp
        ,case when ns.status = 14 then from_unixtime(ns.update_time - 3600) else null end as returned_timestamp
        ,ns.sender_name
        ,ns.sender_phone
        ,ns.sender_address
        ,ns.receiver_name
        ,ns.receiver_phone
        ,ns.receiver_address
        ,sa.first_auto_assign_timestamp
        ,sa.first_incharge_timestamp
        ,sa.last_incharge_timestamp
        ,null as first_confirmed_timestamp
        ,from_unixtime(ns.pick_real_time - 3600) picked_timestamp
        ,0 AS bad_weather_fee
        ,0 AS late_night_service_fee
        ,0 AS holiday_service_fee
        ,assigning_count
        ,weight
        ,quantity


from
(SELECT 
        id
        ,concat('now_ship_',cast(id as VARCHAR)) as uid
        ,code
        ,booking_type
        ,case when booking_type = 3 then cast(referal_id as varchar) else cast(customer_id as varchar) end as customer_id
        ,shipper_id
        ,distance,create_time
        ,status
        ,payment_method
        ,'now_ship' as original_source
        ,city_id
        ,cast(json_extract(extra_data,'$.pick_address_info.district_id') as DOUBLE) as district_id
        ,booking_service_type, pick_real_time, drop_real_time, pick_type, update_time, '' as shopee_order_code,assigning_count
        ,cast(json_extract(extra_data, '$.pick_address_info.address') as varchar) as sender_address
        ,cast(json_extract(extra_data, '$.sender_info.name')as varchar) as sender_name
        ,cast(json_extract(extra_data, '$.sender_info.phone')as varchar) as sender_phone

        ,cast(json_extract(extra_data, '$.drop_address_info.address') as varchar) as receiver_address
        ,cast(json_extract(extra_data, '$.receiver_info.name')as varchar) as receiver_name
        ,cast(json_extract(extra_data, '$.receiver_info.phone')as varchar) as receiver_phone
        ,0 AS weight
        ,0 AS quantity


from shopeefood.foody_express_db__booking_tab__reg_daily_s0_live

UNION

SELECT  
         sbt.id
        ,concat('now_ship_shopee_',cast(id as VARCHAR)) as uid
        ,sbt.code
        ,4 as booking_type
        ,sender_username_v2 as customer_id
        ,shipper_id
        ,distance
        ,create_time
        ,status
        ,1 as payment_method,'now_ship_shopee' as original_source
        ,city_id
        ,cast(json_extract(extra_data,'$.sender_info.district_id') as DOUBLE) as district_id
        ,booking_service_type, pick_real_time, drop_real_time, 1 as pick_type, update_time, shopee_order_code, coalesce(a.assign_cnt,0) as assigning_count
        ,cast(json_extract(extra_data, '$.sender_info.address') as varchar) as sender_address
        ,cast(json_extract(extra_data, '$.sender_info.name')as varchar) as sender_name
        ,cast(json_extract(extra_data, '$.sender_info.phone')as varchar) as sender_phone

        ,cast(json_extract(extra_data, '$.recipient_info.address') as varchar) as receiver_address
        ,cast(json_extract(extra_data, '$.recipient_info.name')as varchar) as receiver_name
        ,cast(json_extract(extra_data, '$.recipient_info.phone')as varchar) as receiver_phone
        ,weight
        ,quantity

from shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live sbt

left join 
(select code,SUM(CAST(JSON_EXTRACT(t.items,'$.weight') AS BIGINT)) AS weight,SUM(CAST(JSON_EXTRACT(t.items,'$.quantity') AS BIGINT)) AS quantity  
from shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live 

cross join unnest (CAST(JSON_EXTRACT(extra_data,'$.items') AS ARRAY<JSON>) ) AS t(items)
GROUP BY 1 ) wt on wt.code = sbt.code 

left join
            (SELECT booking_id, count(booking_id) assign_cnt

            FROM shopeefood.foody_express_db__shopee_booking_change_log_tab__reg_daily_s0_live

            WHERE 1=1
            and message like '%to ASSIGNED%'
            GROUP BY 1
            )a on a.booking_id = sbt.id
)ns

-- location
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = ns.city_id and city.country_id = 86

left join 
(SELECT                     ns.order_id, ns.order_type 
                            ,min(create_time) first_auto_assign_timestamp
                            ,min(case when status in (3,4) then update_time else null end) as first_incharge_timestamp
                            ,max(case when status in (3,4) then update_time else null end) as last_incharge_timestamp
                            ,max(case when status in (3,4) then update_time else null end) as last_picked_timestamp 
                    FROM 
                            ( SELECT ref_order_id AS order_id , order_category as order_type , create_time , update_time, status
                             FROM dev_vnfdbi_opsndrivers.driver_ops_order_assign_log_tab 
                             where order_category in (4,5,6,7) -- now ship/ns shopee/ ns same day

                             )ns
                    GROUP BY 1,2
) sa on sa.order_id = ns.id 
    and (case when ns.booking_type = 2 and ns.booking_service_type = 1 then 4
                when ns.booking_type = 3 and ns.booking_service_type = 1 then 5
                when ns.booking_type = 4 and ns.booking_service_type = 1 then 6
                when ns.booking_type = 2 and ns.booking_service_type = 2 then 7
                when ns.booking_type = 2 and ns.booking_service_type = 3 then 8
                else null end) = sa.order_type 

left join

(SELECT nsc.booking_id
        ,nsc.created_date
        ,nsc.created_timestamp
        ,nsc.booking_type
        ,case when nsc.cancel_type = 6 then 'USER_CANCELLED'
            when nsc.cancel_type = 9 then 'DRIVER_CANCELLED'
            when nsc.cancel_type = 12 then 'SYSTEM_CANCELLED'
            else null end as cancel_type
        ,nsc.cancel_detail
        ,case when nsc.cancel_reason is null then 'No Reason'
            when nsc.cancel_reason in ('Chưa lấy được hàng do thời tiết xấu') then 'Bad weather'
            when nsc.cancel_reason in ('Đặt chuyến/đơn hàng với địa chỉ sai','Booked trip/order with wrong address') then 'Booked trip/order with wrong address'
            when nsc.cancel_reason in ('Thay đổi ý định','Changed mind') then 'Changed mind'
            when nsc.cancel_reason in ('Khách gian lận, có dấu hiệu lừa đảo') then 'Cheating'
            when nsc.cancel_reason in ('Không thể tìm thấy tài xế','Couldn’t find biker','Dịch vụ quá tải, thiếu người giao hàng','Số lượng hàng hóa nhiều, không có shipper đáp ứng') then 'Couldn’t find biker'
            when nsc.cancel_reason in ('Tôi chưa sẵn sàng','I''m not ready') then 'I''m not ready'
            when nsc.cancel_reason in ('Tài xế chưa sẵn sàng đi đơn') then 'Driver not ready'
            when nsc.cancel_reason in ('Sai định vị','Tính sai km') then 'Incorrect route/Incorrect latlng'
            when nsc.cancel_reason in ('Nhầm đơn') then 'Mistaken accept'
            when nsc.cancel_reason in ('Đặt nhầm','Mistaken book') then 'Mistaken book'
            when nsc.cancel_reason in ('Khác','Lý do khác') then 'Others'
            when nsc.cancel_reason in ('Người gửi đóng gói không cẩn thận/ chưa đóng gói xong') then 'Package is not done or not packed properly'
            when nsc.cancel_reason in ('Lí do cá nhân','Lý do cá nhân từ chối toàn bộ đơn') then 'Personal reasons'
            when nsc.cancel_reason in ('Người nhận báo hủy đơn/ không thể nhận hàng') then 'Recipient asks to cancel the order'
            when nsc.cancel_reason in ('Người nhận thay đổi địa chỉ','Khách đổi địa chỉ') then 'Recipient changed delivery address'
            when nsc.cancel_reason in ('Người nhận kiểm tra hàng và không đồng ý nhận hàng') then 'Recipient denied to receive order'
            when nsc.cancel_reason in ('Người nhận hẹn giao lại sau','Khách đổi thời gian ship') then 'Recipient reschedules delivery time'
            when nsc.cancel_reason in ('Người gửi báo hủy đơn/ không thể giao hàng') then 'Sender asks to cancel the order'
            when nsc.cancel_reason in ('Không hỗ trợ ứng tiền mặt','Khách yêu cầu ứng tiền mặt','Số tiền COD quá lớn') then 'Sender requests to get cash in advance'
            when nsc.cancel_reason in ('Không nhận đơn thuộc dạng "Tự động nhận đơn"') then 'Shipper denied Auto Accept order'
            when nsc.cancel_reason in ('Không liên hệ được Người gửi','Không liên hệ được người gửi (sđt thuê bao, sđt sai, địa chỉ sai…)','Không liên hệ được Người nhận'
                                        ,'Không liên hệ được người nhận (sđt thuê bao, sđt sai, địa chỉ sai…)') then 'Unable to contact recipient'
            when nsc.cancel_reason in ('Chờ quá lâu','Wait too long') then 'Wait too long'
            when nsc.cancel_reason in ('Kiện hàng sai tiêu chuẩn về khối lượng/ kích thước.','Hàng cồng kềnh/dễ vỡ') then 'Wrong weight/size input'

            -- new reason
            when nsc.cancel_reason in ('Input wrong/incomplete delivery information','Nhập sai/chưa đầy đủ thông tin giao hàng','Nhập sai/thiếu thông tin giao hàng') then 'Input wrong/incomplete delivery information'
            when nsc.cancel_reason in ('Forget to input promocode','Quên nhập mã khuyến mại') then 'Forget to input promocode'
            when nsc.cancel_reason in ('Change delivery time ','Thay đổi thời gian giao hàng') then 'Change delivery time '
            when nsc.cancel_reason in ('Wait for assigning driver too long','Chờ tìm tài xế quá lâu') then 'Wait for assigning driver too long'
            when nsc.cancel_reason in ('Driver asks for order cancellation','Tài xế yêu cầu hủy đơn') then 'Driver asks for order cancellation'
            when nsc.cancel_reason in ('Driver is too far from pickup point','Tài xế ở quá xa điểm lấy hàng') then 'Driver is too far from pickup point'
            when nsc.cancel_reason in ('Think again on price and fees','Cân nhắc lại về phí giao hàng') then 'Think again on price and fees'
            when nsc.cancel_reason in ('Unable to contact driver','Không liên hệ được với tài xế') then 'Unable to contact driver'
            when nsc.cancel_reason in ('Trùng đơn','Tôi đặt trùng đơn','Made duplicate orders') then 'Made duplicate orders'

            else 'Others' end as cancel_reason
        ,row_number() over(partition by nsc.booking_id order by nsc.created_timestamp DESC) row_num

FROM
    (
    SELECT bc.booking_id
            ,from_unixtime(bc.create_time - 3600) created_timestamp
            ,date(from_unixtime(bc.create_time - 3600)) created_date
            ,bc.booking_type
            ,bc.cancel_type
            ,case 
                when bc.cancel_comment != '' then bc.cancel_comment 
                else COALESCE(cast(json_extract(bc.extra_data,'$.reasons[0].reason_content') as varchar), cast(json_extract(bc.extra_data,'$.cancel_reasons[0]') as varchar)) 
                end as cancel_detail
            ,case when bc.booking_type = 4 then cast(json_extract(bc.extra_data, '$.reasons[0].reason_content') as varchar)
                when bc.booking_type != 4 then cast(json_extract(bc.extra_data,'$.cancel_reasons[0]') as varchar) else null 
                end as cancel_reason

    FROM shopeefood.foody_express_db__booking_cancel_tab__reg_daily_s0_live bc
    )nsc
)nsc on nsc.booking_id = ns.id and nsc.booking_type = ns.booking_type and nsc.row_num = 1

LEFT JOIN
(SELECT 
        bc.booking_id
        ,from_unixtime(bc.create_time - 3600) created_timestamp
        ,date(from_unixtime(bc.create_time - 3600)) created_date
        ,bc.booking_type
        ,bc.cancel_type
        ,case when bc.cancel_comment != '' then bc.cancel_comment else COALESCE(cast(json_extract(bc.extra_data,'$.reasons[0].reason_content') as varchar), cast(json_extract(bc.extra_data,'$.cancel_reasons[0]') as varchar)) end as cancel_detail
        ,case when bc.booking_type = 4 then cast(json_extract(bc.extra_data, '$.reasons[0].reason_content') as varchar)
            when bc.booking_type != 4 then cast(json_extract(bc.extra_data,'$.cancel_reasons[0]') as varchar) else null end as pick_failed_reason
        ,row_number() over(partition by bc.booking_id order by from_unixtime(bc.create_time - 3600) DESC) row_num

FROM shopeefood.foody_express_db__booking_cancel_tab__reg_daily_s0_live bc

where 1=1
and cancel_type in (17)
and booking_type = 4
)pf_reason 
    on pf_reason.booking_id = ns.id 
    and pf_reason.booking_type = ns.booking_type 
    and pf_reason.row_num = 1

WHERE DATE(from_unixtime(ns.create_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day

UNION ALL 
SELECT   
         oct.id
        ,concat('order_delivery_',cast(oct.id as VARCHAR)) as uid
        ,oct.shipper_uid as shipper_id
        ,oct.order_code
        ,null AS shopee_order_code
        ,CAST(oct.uid AS VARCHAR) AS customer_id 
        ,from_unixtime(oct.submit_time - 3600) as created_timestamp
        ,osl.last_cancel_timestamp as canceled_timestamp
        ,date(from_unixtime(oct.submit_time - 3600)) as created_date
        ,oct.distance
        ,case when oct.foody_service_id = 1 then 'order_food'
                when oct.foody_service_id in (5) then 'order_fresh'
                else 'order_market' end as source
        ,0 AS order_type              
        ,case when oct.status = 7 then 'Delivered'
            when oct.status = 8 then 'Cancelled'
            when oct.status = 9 then 'Quit' end as order_status
        ,case 
                when oct.cancel_type in (0,5) and oct.status = 8 then 'System'
                when oct.cancel_type = 1 and oct.status = 8 then 'CS BPO'
                when oct.cancel_type = 2 and oct.status = 8 then 'User'
                when oct.cancel_type in (3,4) and oct.status = 8 then 'Merchant'
                when oct.cancel_type = 6 and oct.status = 8 then 'Fraud'
                end as cancel_by              
        ,city.name_en as city_name
        ,case when oct.city_id = 217 then 'HCM'
            when oct.city_id = 218 then 'HN'
            when oct.city_id = 219 then 'DN'
            ELSE 'OTH' end as city_group
        ,oct.city_id
        ,oct.district_id
        ,oct.is_asap
        ,case when oct.status not in (8) then null
                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Quán đã đóng cửa','Quán đóng cửa','Shop was closed','Shop is closed','Shop was no longer operating','Quán cúp diện','Driver reported Merchant closed','Tài xế báo Quán đóng cửa') then 'Shop closed'
                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('No driver','không có driver','Đơn hàng chưa có Tài xế nhận','No Drivers found','Không có tài xế nhận giao hàng','Lack of shipper','I will not wait any longer','Tôi không muốn tiếp tục đợi') then 'No driver'
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
                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Merchant cannot accept the order at the moment','Hiện tại Quán không thể tiếp nhận thêm đơn') then 'Merchant overload'
                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) = '' then 'Others'
                when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) is null then 'Others'
                else 'Others' end as cancel_reason
        ,null AS pickup_failed_reason
        ,DATE_DIFF('second',FROM_UNIXTIME(oct.submit_time - 3600),osl.last_cancel_timestamp)*1.00/60 AS lt_submit_to_cancel 
        ,CASE 
             WHEN final_delivered_time = 0 THEN NULL
             ELSE DATE_DIFF('second',FROM_UNIXTIME(oct.submit_time - 3600),FROM_UNIXTIME(oct.final_delivered_time - 3600)) END AS lt_deliver
        ,null AS lt_return
        ,CASE 
             WHEN final_delivered_time = 0 THEN NULL
             ELSE FROM_UNIXTIME(oct.final_delivered_time - 3600) END AS delivered_timestamp              
        ,null AS returned_timestamp
        ,mm.merchant_name AS sender_name 
        ,null AS sender_phone 
        ,mm.address_text AS sender_address
        ,customer_username AS receiver_name
        ,customer_phone AS receiver_phone
        ,null AS receiver_address
        ,osl.first_auto_assign_timestamp
        ,osl.first_incharge_timestamp
        ,osl.last_incharge_timestamp
        ,osl.first_confirmed_timestamp
        ,from_unixtime(go.pick_timestamp) AS last_picked_timestamp
        ,go.bad_weather_fee
        ,go.late_night_service_fee
        ,go.holiday_service_fee
        ,sa.assigning_count
        ,0 AS weight 
        ,cast(json_extract(oct.extra_data,'$.total_item') as bigint) as quantity
        -- ,case when oct.payment_method = 1 then 'Cash'
        --         when oct.payment_method = 6 then 'AP'
        --         when oct.payment_method = 4 then 'Card'
        --         when oct.payment_method = 8 then 'VNPay/ibanking'
        --         when oct.payment_method = 12 then 'AP credit card'
        --         when oct.payment_method = 3 then 'Bank transfer'
        --         when oct.payment_method = 7 then 'Momo'
        --         else 'Others' end as payment_method
        
        -- ,case when oct.merchant_paid_method = 6 and oct.status in (7) then 1 else 0 end as is_nmw
        -- ,go.is_now_merchant_order_flag

    from (SELECT * FROM shopeefood.shopeefood_mart_dwd_vn_order_completed_da WHERE date(dt) = current_date - interval '1' day) oct
    -- left join shopeefood.foody_mart__profile_shipper_master shp on shp.shipper_id = oct.shipper_uid
    left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86

    left join (select 
                      ref_order_id
                     ,order_category
                     ,COUNT(DISTINCT (ref_order_id,create_time)) AS assigning_count
                from dev_vnfdbi_opsndrivers.driver_ops_order_assign_log_tab
                WHERE order_category = 0
                GROUP BY 1,2
                ) sa on sa.ref_order_id = oct.id 


    left join (select * from shopeefood.foody_mart__fact_gross_order_join_detail )go on go.id = oct.id 
    Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = oct.district_id
    left join shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr on cr.id = try_cast(json_extract_scalar(oct.extra_data,'$.note_ids') as int) -- note_ids: cancel_reason
    left join shopeefood.foody_mart__profile_merchant_master mm 
        on mm.merchant_id = oct.restaurant_id
        and TRY_CAST(mm.grass_date AS DATE) = DATE(FROM_UNIXTIME(oct.submit_time - 3600))


    -- assign time: request archive log
    left join
    (SELECT order_id
        ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
        ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
        ,min(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_incharge_timestamp
        ,max(case when status = 7 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_delivered_timestamp
        ,min(case when status = 13 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_confirmed_timestamp
        ,max(case when status = 8 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_cancel_timestamp
        ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
    from shopeefood.foody_order_db__order_status_log_tab_di
    where 1=1
    group by order_id
    )osl on osl.order_id = oct.id


    WHERE 1=1
    and date(from_unixtime(oct.submit_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day

)
,peak_mode_tab as 
(select 
         date(from_unixtime(a.start_time - 3600)) as created_date
        ,from_unixtime(a.start_time - 3600) as start_ts
        ,from_unixtime((a.start_time + running_time) - 3600) as end_ts
        ,running_time/cast(60 as double) as running_ts
        ,assigning_order
        ,available_driver 
        ,online_driver
        ,a.city_id
        ,a.district_id
        ,b.name as peak_mode_name
        ,case when b.name in ('Peak 1 Mode','Peak 2 Mode','Peak 3 Mode') then b.name
             else 'Normal Mode' end as peak_mode  

from shopeefood.foody_delivery_admin_db__peak_mode_export_activity_tab__reg_daily_s0_live a 

left join shopeefood.foody_delivery_admin_db__peak_mode_tab__reg_daily_s0_live b on b.id = a.mode_id

WHERE date(from_unixtime(a.start_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
)
SELECT 
        raw.*
       ,arrive.max_arrived_at_buyer_timestamp
       ,arrive.max_arrived_at_merchant_timestamp 
    --    ,DATE(sa.create_time) AS inflow_date              
    --    ,sa.create_time AS last_incharge_timestamp   
       ,sa.assign_type
       ,sa.order_type AS order_assign_type
       ,pm.peak_mode as peak_mode_name
       ,dot.id AS delivery_id
       ,FROM_UNIXTIME(dot.estimated_pick_time - 3600) AS eta_pick_time
       ,FROM_UNIXTIME(dot.estimated_drop_time - 3600) AS eta_drop_time
       ,dot.pick_latitude
       ,dot.pick_longitude
       ,dot.drop_latitude
       ,dot.drop_longitude
       ,dot.group_id

 
FROM raw 

LEFT JOIN (SELECT * FROM  shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day ) dot 
    on dot.ref_order_id = raw.id
    and dot.ref_order_category = raw.order_type

LEFT JOIN

(SELECT order_id
,max(case when destination_key = 256 then from_unixtime(create_time - 60*60) else null end) max_arrived_at_merchant_timestamp
,max(case when destination_key = 512 then from_unixtime(create_time - 60*60) else null end) max_arrived_at_buyer_timestamp

FROM shopeefood.foody_partner_db__driver_order_arrive_log_tab__reg_daily_s0_live doal
WHERE 1=1
AND DATE(FROM_UNIXTIME(create_time - 3600)) between current_date - interval '30' day and current_date - interval '1' day
group by 1
)arrive on dot.id = arrive.order_id

LEFT JOIN peak_mode_tab pm 
    on pm.city_id = raw.city_id 
    and pm.district_id = raw.district_id
    and raw.created_timestamp > pm.start_ts 
    and raw.created_timestamp < pm.end_ts


LEFT JOIN assignment sa 
    on sa.ref_order_id = raw.id 
    and sa.order_category = raw.order_type

-- take last incharged
LEFT JOIN assignment sa_filter
    on  sa.ref_order_id = sa_filter.ref_order_id          
    and sa.order_category = sa_filter.order_category 
    and sa.create_time < sa_filter.create_time

WHERE 1 = 1
AND sa_filter.order_id is null

