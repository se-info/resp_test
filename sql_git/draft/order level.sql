SELECT
    created_time
    ,source
    ,food_service
    ,order_id
    ,order_code
    ,is_asap
    ,shipper_id
    ,order_status

    ,peak_mode_name
    ,pick_city_name
    ,pick_district_name
    ,drop_city_name
    ,drop_district_name
    ,delivery_distance
--     ,is_ignored_or_denied_before
    ,if(order_status != 'Cancelled', NULL, coalesce(cancel_reason_nowship, cancel_reason_nowfood)) as cancel_reason
    ,is_group_order
    ,is_stack_order
    ,assign_time
--     ,incharge_time
    , if(assign_time <= incharge_time, cast(date_diff('second', assign_time, incharge_time) as double) / 60, null) as incharge_period
    , if(arrived_at_merchant_time <= pick_time, cast(date_diff('second', arrived_at_merchant_time, pick_time) as double) / 60, null) as wait_time_at_merchant_period
--     ,arrived_at_merchant_time
    ,eta
--     ,pick_time
--     ,drop_time
    , if(created_time <= drop_time, cast(date_diff('second', created_time, drop_time) as double) / 60, null) as completion_time
    ,is_order_qualified_hub
    ,is_order_in_hub_shift
    ,if(is_order_in_hub_shift = 1, hub_type, null) as hub_type
    ,if(is_order_in_hub_shift = 1, hub_name, null) as hub_name
    ,bad_weather_fee
    ,total_income

FROM
(SELECT DISTINCT
    base.created_time
    ,base.source
    ,base.food_service
    ,base.order_id
    ,base.order_code
    ,base.is_asap
    ,base.shipper_id
    ,base.order_status

    ,base.peak_mode_name
    ,base.pick_city_name
    ,base.pick_district_name
    ,base.drop_city_name
    ,base.drop_district_name
    ,base.delivery_distance
--     ,base.is_ignored_or_denied_before
    ,base.cancel_reason_nowfood
    ,base.cancel_reason_nowship
    ,base.is_group_order
    ,base.is_stack_order
    ,base.assign_time
    ,base.incharge_time
    ,base.max_arrived_at_merchant_timestamp as arrived_at_merchant_time
    ,base.estimated_delivered_time AS eta
    ,base.pick_time
    ,base.drop_time
    ,case
        when coalesce(hub_info.id,0) > 0 then hub_info.hub_name
        when (coalesce(pick_hub_info.id,0) > 0 and base.delivery_distance <= 2) then pick_hub_info.pick_hub_name
    else null end as is_order_qualified_hub
    ,case when base.report_date between date('2021-07-09') and date('2021-10-05') and is_hub_driver = 1 and base.city_id = 217 then 1
            when base.report_date between date('2021-07-24') and date('2021-10-04') and is_hub_driver = 1 and base.city_id = 218 then 1
            when base.driver_payment_policy = 2 then 1
    else 0 end as is_order_in_hub_shift
    , case
        when driver_type.start_shift = 0 and driver_type.end_shift = 23 then null
        when driver_type.end_shift - driver_type.start_shift = 10 then 'HUB-10'
        when driver_type.end_shift - driver_type.start_shift = 8 then 'HUB-08'
        when driver_type.end_shift - driver_type.start_shift = 5 then 'HUB-05'
        when driver_type.end_shift - driver_type.start_shift = 3 and driver_type.start_shift = 10 then 'HUB-03S'
        when driver_type.end_shift - driver_type.start_shift = 3 and driver_type.start_shift != 10 then 'HUB-03C'
        when driver_type.end_shift - driver_type.start_shift = 4 and driver_type.end_shift = 21 then 'HUB-03C'
    else null end as hub_type
    ,d.hub_name
    ,base.bad_weather_fee
    ,bill_fee.total_income

FROM
(SELECT
--       date(from_unixtime(dot.submitted_time- 3600)) created_date
--       ,hour(from_unixtime(dot.submitted_time- 3600)) created_hour
      from_unixtime(dot.submitted_time- 3600) as created_time
      ,case when dot.estimated_drop_time = 0 then null else from_unixtime(dot.estimated_drop_time - 3600) end as estimated_delivered_time
      ,case when dot.ref_order_category = 0 then 'order_delivery'
            when dot.ref_order_category = 3 then 'now_moto'
            when dot.ref_order_category = 4 then 'now_ship_customer'
            when dot.ref_order_category = 5 then 'now_ship_merchant'
            when dot.ref_order_category = 6 then 'now_ship_shopee'
            when dot.ref_order_category = 7 then 'now_ship_sameday'
            when dot.ref_order_category = 8 then 'now_ship_multi_drop'
            else null end source
      ,case when COALESCE(oct.foody_service_id,0) = 1 then 'Food'
            when COALESCE(oct.foody_service_id,0) in (5) then 'Fresh'
            when COALESCE(oct.foody_service_id,0) > 0 then 'Market'
            when dot.ref_order_category = 0 then 'Food'
            else 'NowShip' end as food_service
      ,dot.ref_order_id as order_id
      ,dot.ref_order_code as order_code
      ,dot.uid as shipper_id
      ,dot.is_asap
      ,case when dot.order_status = 400 then 'Delivered'
            when dot.order_status = 401 then 'Quit'
            when dot.order_status in (402,403,404) then 'Cancelled'
            when dot.order_status in (405) then 'Returned'
            else 'Others' end as order_status
      ,case when dot.order_status = 1 then 'Pending'
            when dot.order_status in (100,101,102) then 'Assigning'
            when dot.order_status in (200,201,202,203,204) then 'Processing'
            when dot.order_status in (300,301) then 'Error'
            when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
            else null end as order_status_group

      ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
            when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
            else date(from_unixtime(dot.submitted_time- 3600)) end as report_date

      ,mode.peak_mode_name
      ,IF(dot.pick_city_id = 238, 'Dien Bien', city_pick.name_en) as pick_city_name
      ,district_pick.name_en as pick_district_name
      ,dot.pick_city_id as city_id
      ,IF(dot.drop_city_id = 238, 'Dien Bien', city_drop.name_en) as drop_city_name
      ,district_drop.name_en as drop_district_name
      ,cast(dot.delivery_distance as double)/1000 as delivery_distance
      ,if(driver_hub.shipper_type_id = 12, 1, 0) as is_hub_driver
--       ,if(dod.order_id is not null or fa.last_ignored_timestamp is not null, 1, 0) as is_ignored_or_denied_before
      ,case when oct.status not in (8) then null
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
            else 'Others' end as cancel_reason_nowfood
      ,case when ns.status in (6,9,12)
                            then case
                                        when ns.source in('now_ship_shopee','spx_portal') then case when ns.order_status = 'Assigning Timeout' then 'No driver' else coalesce(nsc.cancel_reason,'No Reason') end
                                        when ns.source in ('now_ship_user','now_ship_merchant') then case when assign.last_incharge_timestamp is null and ns.assigning_count > 0 and nsc.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') then 'No driver' else coalesce(nsc.cancel_reason,'No Reason') end
                                        when ns.source in ('now_ship_same_day') then case when assign.last_incharge_timestamp is null and nsc.cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') then 'No driver' else coalesce(nsc.cancel_reason,'No Reason') end
                                else coalesce(nsc.cancel_reason,'No Reason') end
      else null end as cancel_reason_nowship
      ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time = ogi.create_time then 1 else 0 end as is_group_order
      ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 1
            when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time != ogi.create_time then 1
            else 0 end as is_stack_order
      ,fa.first_auto_assign_timestamp as assign_time
      ,fa.last_incharge_timestamp as incharge_time
      ,fa.last_picked_timestamp as pick_time
      ,if(arrive.max_arrived_at_merchant_timestamp is not null, arrive.max_arrived_at_merchant_timestamp, fa.last_picked_timestamp) as max_arrived_at_merchant_timestamp
      ,if(dot.real_drop_time = 0, null, from_unixtime(dot.real_drop_time - 3600)) as drop_time
      ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
      ,coalesce(cast(json_extract(oct.extra_data,'$.bad_weather_fee.user_pay_amount') as double), 0) as bad_weather_fee -- user_bwf
      ,coalesce(cast(json_extract(dotet.order_data,'$.hub_id') as bigint), 0) as hub_id
      ,coalesce(cast(json_extract(dotet.order_data,'$.pick_hub_id') as bigint), 0) as pick_hub_id

FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on oct.id = dot.ref_order_id and dot.ref_order_category = 0 and date(from_unixtime(oct.submit_time-3600)) BETWEEN {{start_date}} - interval '2' day and {{end_date}} + interval '2' day
left join shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr on cr.id = try_cast(json_extract_scalar(oct.extra_data,'$.note_ids') as int) -- note_ids: cancel_reason
left join (SELECT ns.id as order_id
                ,ns.booking_type
                ,ns.status
                ,ns.assigning_count
                ,case when ns.status = 11 then 'Delivered'
                      when ns.status in (6,9,12) then 'Cancelled'
                      when ns.booking_type in (4,5) and ns.status = 17 then 'Pickup Failed'
                      when ns.booking_type not in (4,5) and ns.status = 23 then 'Pickup Failed'
                      when ns.status = 14 then 'Returned'
                      when ns.booking_type in (4,5) and ns.status = 3 then 'Assigning Timeout'
                      else 'Others' end as order_status
                ,case when ns.booking_type = 2 and ns.booking_service_type = 1 then 4
                      when ns.booking_type = 3 and ns.booking_service_type = 1 then 5
                      when ns.booking_type = 4 and ns.booking_service_type = 1 then 6
                      when ns.booking_type = 2 and ns.booking_service_type = 2 then 7
                      else 10 end as order_type
               , case when ns.booking_type = 2 and ns.booking_service_type = 1 then 'now_ship_user'
                  when ns.booking_type = 3 and ns.booking_service_type = 1 then 'now_ship_merchant'
                  when ns.booking_type = 4 and ns.booking_service_type = 1 then 'now_ship_shopee'
                  when ns.booking_type = 5 and ns.booking_service_type = 1 then 'spx_portal'
                  when ns.booking_type = 2 and ns.booking_service_type = 2 then 'now_ship_same_day'
                  when ns.booking_type = 2 and ns.booking_service_type = 3 then 'now_ship_multi_drop'
                  else null end as source
               , from_unixtime(ns.create_time - 3600) as created_timestamp
        FROM (SELECT id,concat('now_ship_',cast(id as VARCHAR)) as uid, booking_type,shipper_id, distance,code,create_time, status, payment_method,'now_ship' as original_source,city_id,cast(json_extract(extra_data,'$.pick_address_info.district_id') as DOUBLE) as district_id , pick_real_time,drop_real_time,shipping_fee
                     ,booking_service_type, assigning_count
                    from shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live

                UNION ALL

            SELECT sbt.id,concat('now_ship_shopee_',cast(sbt.id as VARCHAR)) as uid, 4 as booking_type, sbt.shipper_id,sbt.distance,sbt.code,sbt.create_time,sbt.status,1 as payment_method,'now_ship_shopee' as original_source,city_id,cast(json_extract(sbt.extra_data,'$.sender_info.district_id') as DOUBLE) as district_id, sbt.pick_real_time,sbt.drop_real_time,sbt.shipping_fee
                  ,sbt.booking_service_type, coalesce(a.assign_cnt,0) as assigning_count
                from shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live  sbt

                    left join
                             (SELECT booking_id, count(booking_id) assign_cnt

                              FROM shopeefood.foody_express_db__shopee_booking_change_log_tab__reg_daily_s0_live

                              WHERE 1=1
                              and message like '%to ASSIGNED%'
                              GROUP BY 1
                             )a on a.booking_id = sbt.id

            ) ns
        ) ns  on dot.ref_order_id = ns.order_id and dot.ref_order_category = ns.order_type
left join (SELECT nsc.booking_id
              ,nsc.created_date
              ,nsc.created_timestamp
              ,nsc.booking_type
              ,case when nsc.cancel_type = 6 then 'USER_CANCELLED'
                    when nsc.cancel_type = 9 then 'DRIVER_CANCELLED'
                    when nsc.cancel_type = 12 then 'SYSTEM_CANCELLED'
                    else null end as cancel_type
              ,nsc.cancel_detail
              ,case when nsc.cancel_reason is null then 'No Reason'
                    when lower(nsc.cancel_reason) LIKE '%thời tiết xấu%' then 'Bad weather'
                    when nsc.cancel_reason in ('Đặt chuyến/đơn hàng với địa chỉ sai','Đặt chuyến với địa chỉ sai','Booked trip/order with wrong address') then 'Booked trip/order with wrong address'
                    when nsc.cancel_reason in ('Thay đổi ý định','Changed mind') then 'Changed mind'
                    when nsc.cancel_reason in ('Khách gian lận, có dấu hiệu lừa đảo') then 'Cheating'
                    when nsc.cancel_reason in ('Không thể tìm thấy tài xế','Couldn’t find biker','Dịch vụ quá tải, thiếu người giao hàng','Số lượng hàng hóa nhiều, không có shipper đáp ứng') then 'Couldn’t find biker'
                    when nsc.cancel_reason in ('Tôi chưa sẵn sàng','I''m not ready') then 'I''m not ready'
                    when nsc.cancel_reason in ('Tài xế chưa sẵn sàng đi đơn') then 'Driver not ready'
                    when nsc.cancel_reason in ('Sai định vị','Định vị sai','Tính sai km') then 'Incorrect route/Incorrect latlng'
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
                          ,case when bc.cancel_comment != '' then bc.cancel_comment else COALESCE(cast(json_extract(bc.extra_data,'$.reasons[0].reason_content') as varchar), cast(json_extract(bc.extra_data,'$.cancel_reasons[0]') as varchar)) end as cancel_detail
                          ,case when bc.booking_type = 4 then cast(json_extract(bc.extra_data, '$.reasons[0].reason_content') as varchar)
                                when bc.booking_type != 4 then cast(json_extract(bc.extra_data,'$.cancel_reasons[0]') as varchar) else null end as cancel_reason


                    FROM shopeefood.foody_express_db__booking_cancel_tab__reg_daily_s0_live bc
                    )nsc
        )nsc on nsc.booking_id = dot.ref_order_id and nsc.booking_type = ns.booking_type and nsc.row_num = 1

--- city, district of pick
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city_pick on city_pick.id = dot.pick_city_id AND city_pick.country_id = 86
left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district_pick on district_pick.id = dot.pick_district_id and district_pick.province_id = dot.pick_city_id
--- city, district of drop
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city_drop on city_drop.id = dot.drop_city_id AND city_drop.country_id = 86
left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district_drop on district_drop.id = dot.drop_district_id and district_drop.province_id = dot.drop_city_id

left join
            (SELECT  order_id , 0 as order_type
                    ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                    ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                    ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
--                     ,max(case when status = 12 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_ignored_timestamp
                    from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                    where 1=1
                    and grass_schema = 'foody_order_db'
                    group by 1,2

            UNION ALL

            SELECT   ns.order_id, ns.order_type
                    ,min(from_unixtime(create_time - 3600)) first_auto_assign_timestamp
                    ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                    ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
--                     ,max(case when status in (8,9,17,18) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_ignored_timestamp
            FROM
                    ( SELECT order_id, order_type , create_time , update_time, status
                     from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                     where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                     and grass_schema = 'foody_partner_archive_db'

                     UNION

                     SELECT order_id, order_type, create_time , update_time, status
                     from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                     where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                     and schema = 'foody_partner_db'
                     )ns
            GROUP BY 1,2
            )fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type

-- left join shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod on dod.order_id = dot.id and dod.deny_type = 1 -- Driver fault
-- stack group_code
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                    and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                    and ogm_filter.create_time >  ogm.create_time
LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id

LEFT JOIN
    (SELECT
        a.order_id
        ,a.order_type
        ,case when a.order_type <> 200 then order_type else ogi.ref_order_category end as order_category
        ,case when a.assign_type = 1 then '1. Single Assign'
              when a.assign_type in (2,4) then '2. Multi Assign'
              when a.assign_type = 3 then '3. Well-Stack Assign'
              when a.assign_type = 5 then '4. Free Pick'
              when a.assign_type = 6 then '5. Manual'
              when a.assign_type in (7,8) then '6. New Stack Assign'
              else null end as assign_type
        ,from_unixtime(a.create_time - 3600) as create_time
        ,from_unixtime(a.update_time - 3600) as update_time

    from (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

            from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
            where status in (3,4) -- shipper incharge

            UNION ALL

            SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

            from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
            where status in (3,4) -- shipper incharge
        )a

        -- take last incharge
        LEFT JOIN
                (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

                from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge

                UNION ALL

                SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

                from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge
            )a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time

        LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end

where 1=1
and a_filter.order_id is null -- take last incharge
and a.order_type = 200
GROUP BY 1,2,3,4,5,6

) group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category
LEFT JOIN
    (SELECT ogm.*, ogi.create_time as group_create_time
    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
    ) group_info on group_info.order_id = dot.id and group_info.mapping_status = 11 and group_info.group_id = dot.group_id
LEFT JOIN
    (
        SELECT   ns.order_id, ns.order_type, ns.order_category ,min(from_unixtime(ns.create_time - 3600)) first_auto_assign_timestamp
                ,max(case when status in (3,4) then cast(from_unixtime(ns.update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,count(ns.order_id) no_assign
                ,count(case when status in (3,4) then order_id else null end) no_incharged
                ,count(case when status in (8,9,17,18) then order_id else null end) no_ignored
                ,count(case when status in (2,14,15) then order_id else null end) no_deny
                ,count(case when status in (13) then order_id else null end) no_shipper_checkout
                ,count(case when status in (16) then order_id else null end) no_incharge_error
                ,count(case when status in (7) then order_id else null end) no_system_error

        FROM
        (
        SELECT *
                ,case when ns.order_type = 0 then '1. Food/Market'
                        when ns.order_type = 4 then '2. NowShip Instant'
                        when ns.order_type = 5 then '3. NowShip Food Mex'
                        when ns.order_type = 6 then '4. NowShip Shopee'
                        when ns.order_type = 7 then '5. NowShip Same Day'
                        when ns.order_type = 8 then '6. NowShip Multi Drop'
                        when ns.order_type = 200 and ogi.ref_order_category = 0 then '1. Food/Market'
                        when ns.order_type = 200 and ogi.ref_order_category = 6 then '4. NowShip Shopee'
                        when ns.order_type = 200 and ogi.ref_order_category = 7 then '5. NowShip Same Day'
                        else 'Others' end as order_source
                ,case when ns.order_type <> 200 then ns.order_type else ogi.ref_order_category end as order_category
                ,case when ns.order_type = 200 then 'Group Order' else 'Single Order' end as order_group_type
        FROM
                ( SELECT order_id, order_type , create_time , assign_type, update_time, status

                 from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                 where order_type in (4,5,6,7,8,200) -- now ship/ns shopee/ ns same day
                 and status in (3,4,7,8,9,2,13,14,15,16,17,18)

                 UNION

                 SELECT order_id, order_type, create_time , assign_type, update_time, status

                 from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                 where order_type in (4,5,6,7,8,200) -- now ship/ns shopee/ ns same day
                 and status in (3,4,7,8,9,2,13,14,15,16,17,18)
                 )ns
        LEFT JOIN (select id, ref_order_category from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) group by 1,2) ogi on ogi.id > 0 and ogi.id = case when ns.order_type = 200 then ns.order_id else 0 end

        LEFT JOIN
                    (SELECT ogm.group_id
                           ,ogi.group_code
                           ,count (distinct ogm.ref_order_id) as total_order_in_group
                     FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm

                     LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
                     WHERE 1=1
                     and ogm.group_id is not null

                     GROUP BY 1,2
                     )order_rank on order_rank.group_id = case when ns.order_type = 200 then ns.order_id else 0 end

        )ns
        WHERE 1=1
        GROUP BY 1,2,3
        )assign on assign.order_id = case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and group_info.create_time = group_info.group_create_time then dot.group_id else ns.order_id end
               and assign.order_type = case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and group_info.create_time = group_info.group_create_time then coalesce(group_order.order_type,0) else ns.order_type end
               and assign.order_category = ns.order_type and ns.created_timestamp <= assign.first_auto_assign_timestamp
--- find whether driver is hub driver or not
left join
        (SELECT  sm.shipper_id
                ,sm.shipper_type_id
                ,try_cast(sm.grass_date as date) as report_date
                from shopeefood.foody_mart__profile_shipper_master sm
                where 1=1
                and shipper_type_id <> 3
                and shipper_status_code = 1
                and grass_region = 'VN'
                GROUP BY 1,2,3
        ) driver_hub on driver_hub.shipper_id = dot.uid and driver_hub.report_date = case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
                                                                                         when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
                                                                                         else date(from_unixtime(dot.submitted_time- 3600)) end
---arrive at buyer/merchant timestamp
LEFT JOIN
        (SELECT order_id
                ,max(case when destination_key = 256 then from_unixtime(create_time - 3600) else null end) max_arrived_at_merchant_timestamp
                ,max(case when destination_key = 512 then from_unixtime(create_time - 3600) else null end) max_arrived_at_buyer_timestamp

            FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_arrive_log_tab_vn_da where date(dt) = current_date - interval '1' day) doal

            WHERE 1=1
            and grass_schema = 'foody_partner_db'
            group by 1
        ) arrive on dot.id = arrive.order_id
LEFT JOIN
        (SELECT
            pm.city_id
            , pm.district_id
            , pm.mode_id
            , from_unixtime(pm.start_time - 3600) AS start_time
            , from_unixtime(pm.start_time + pm.running_time - 3600) AS end_time
            , pm.available_driver
            , pm.assigning_order
            , pm.driver_availability
            , pm_name.name AS peak_mode_name

            FROM shopeefood.foody_delivery_admin_db__peak_mode_export_activity_tab__reg_daily_s0_live pm
            LEFT JOIN shopeefood.foody_delivery_admin_db__peak_mode_tab__reg_daily_s0_live pm_name ON pm_name.id = pm.mode_id
            WHERE pm.mode_id in (7,8,9,10,11)
        ) mode ON mode.city_id = dot.pick_city_id AND mode.district_id = dot.pick_district_id AND from_unixtime(dot.submitted_time- 3600) >= mode.start_time AND from_unixtime(dot.submitted_time- 3600) < mode.end_time
WHERE 1=1
and ogm_filter.create_time is null
and dot.pick_city_id <> 238
and dot.order_status in (400,401,402,403,404,405,406,407) -- Completed
and date(from_unixtime(dot.submitted_time- 3600)) between {{start_date}} and {{end_date}}
and IF(dot.pick_city_id = 238, 'Dien Bien', city_pick.name_en) in {{pick_city_name}}
and IF(dot.drop_city_id = 238, 'Dien Bien', city_drop.name_en) in {{drop_city_name}}
and case when COALESCE(oct.foody_service_id,0) = 1 then 'Food'
            when COALESCE(oct.foody_service_id,0) in (5) then 'Fresh'
            when COALESCE(oct.foody_service_id,0) > 0 then 'Market'
            when dot.ref_order_category = 0 then 'Food'
            when dot.ref_order_category = 3 then 'Now Moto'
            when dot.ref_order_category = 4 then 'NowShip Instant (customer)'
            when dot.ref_order_category = 5 then 'NowShip Instant (merchant)'
            when dot.ref_order_category = 6 then 'NowShip Shopee'
            when dot.ref_order_category = 7 then 'NowShip Sameday'
            when dot.ref_order_category = 8 then 'NowShip Multi drop'
    else null end in {{service}}
) base
LEFT JOIN
    (SELECT
        id
        , hub_name
        ,case when city_id = 217 then 'HCM'
            when city_id = 218 then 'HN'
            when city_id = 219 then 'DN'
            ELSE 'OTH' end as hub_location
    FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
    WHERE 1=1
    and id <> 2
    and driver_count > 0
    ) hub_info on hub_info.id = base.hub_id

LEFT JOIN
    (SELECT
        id
        , hub_name as pick_hub_name
        ,case when city_id = 217 then 'HCM'
            when city_id = 218 then 'HN'
            when city_id = 219 then 'DN'
            ELSE 'OTH' end as pick_hub_location
    FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
    WHERE 1=1
    and id <> 2
    and driver_count > 0
    ) pick_hub_info on pick_hub_info.id = base.pick_hub_id
LEFT JOIN -- income

(SELECT raw2.order_id
    ,raw2.partner_id
    ,raw2.source
    ,raw2.food_service
--     ,raw2.sub_source
    ,raw2.total_shipping_fee
    ,coalesce(raw2.shipping_fee_share,0) as shipping_fee_share -- add bad weather fee
    ,coalesce(raw2.return_fee_share,0) as return_fee_share
    ,coalesce(raw2.additional_bonus,0) as additional_bonus
    ,coalesce(raw2.order_completed_bonus,0) as order_completed_bonus
    ,coalesce(raw2.other_payables,0) as other_payables
    ,coalesce(raw2.bad_weather_fee_temp,0) as bad_weather_fee
    ,coalesce(raw2.other_adjustment,0) as other_adjustment
    ,coalesce(raw2.late_night_fee_temp,0) as late_night_fee
    ,coalesce(raw2.holiday_fee_temp,0) as holiday_fee
    ,coalesce(raw2.hub_cost_auto,0) as hub_cost_auto
    ,coalesce(raw2.hub_adjustment,0) as hub_adjustment
    ,coalesce(raw2.shipping_fee_share,0)
         + coalesce(raw2.return_fee_share,0)
         + coalesce(raw2.additional_bonus,0)
         + coalesce(raw2.order_completed_bonus,0)
         + coalesce(raw2.other_payables,0)
         + coalesce(raw2.bad_weather_fee_temp,0)
         + coalesce(raw2.late_night_fee_temp,0)
         + coalesce(raw2.holiday_fee_temp,0)
         + coalesce(raw2.hub_cost_auto,0)
         + coalesce(raw2.hub_adjustment,0)
         + coalesce(raw2.other_adjustment,0)
    AS total_income

    from
    (Select raw.order_id
    ,raw.partner_id
    ,raw.date_
    ,raw.year_week
    ,raw.city_name
    ,raw.partner_type as shipper_type_id
    ,case when raw.city_name in ('HCM','HN') then
            case when raw.partner_type = 1 then 0 -- 'full_time'
                when raw.partner_type = 3 then 0 -- 'tester'
            else 1
            end
        else 0 end as is_new_policy

    ,case when raw.city_name in ('HCM','HN') then
            case when raw.partner_type = 1 then 'full_time'
                when raw.partner_type = 3 then 'tester'
                when raw.partner_type = 12 then 'part_time_17'
            else 'driver_new_policy'
            end
        when raw.partner_type = 1 then 'full_time'
        when raw.partner_type = 2 then 'part_time'
        when raw.partner_type = 3 then 'tester'
        when raw.partner_type = 6 then 'part_time_09'
        when raw.partner_type = 7 then 'part_time_11'
        when raw.partner_type = 8 then 'part_time_12'
        when raw.partner_type = 9 then 'part_time_14'
        when raw.partner_type = 10 then 'part_time_15'
        when raw.partner_type = 11 then 'part_time_16'
        when raw.partner_type = 12 then 'part_time_17'
        else 'others' end as shipper_type
    ,raw.total_shipping_fee
    ,raw.total_shipping_fee_basic
    ,raw.total_shipping_fee_surge
    ,raw.bad_weather_cost_driver_new
    ,case when raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3 then raw.bad_weather_cost_driver_new else 0 end as bad_weather_cost_driver_new_hub
    ,raw.bad_weather_cost_driver_new - (case when raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3 then raw.bad_weather_cost_driver_new else 0 end) as bad_weather_cost_driver_new_non_hub

    ,raw.user_bwf
    ,raw.total_return_fee
    ,raw.total_return_fee_basic
    ,raw.total_return_fee_surge
--     ,raw.source
--     ,raw.sub_source
    ,raw.source
    ,raw.food_service
    ,raw.distance
    ,raw.status
    ,raw.rev_shipping_fee
    ,raw.prm_cost
    ,raw.rev_cod_fee
    ,raw.rev_return_fee

    ,count(DISTINCT raw.order_id) as total_bill
    ,count(distinct case when raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3 then raw.order_id else null end) as total_bill_hub
    ,sum(raw.total_shipping_fee_collected_from_customer) as total_shipping_fee_collected_from_customer
    ,SUM(case when trx.txn_type in (906,907) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as hub_cost_auto
    ,SUM(case when trx.txn_type in (501,518,512,560,900,901)
                and trim(trx.note) not in ('HUB_MODEL_EXTRASHIP_Bù thu nhập do lỗi hệ thống','HUB_MODEL_EXTRASHIP_Chưa nhận auto pay do sup hub điều chỉnh ca trong shift','HUB_MODEL_EXTRASHIP_Lỗi sai thu nhập do Work Schedules',
                                            'HUB_MODEL_EXTRASHIP_Điều chỉnh thu nhập do miss config')
                and (trim(trx.note) in ('HUB_MODEL_SHIP_30/04','HUB_MODEL_SHIP_05/05') or (date(from_unixtime(trx.create_time - 60*60))  > date('2021-11-11') and (trim(trx.note) LIKE 'HUB_MODEL_EXTRASHIP%' or trim(trx.note) LIKE '%HUB_MODEL_DAILYBONUS%')))
        then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as hub_adjustment
    ,SUM(case when trx.txn_type in (565,518)
                    and (trx.note = 'ADJUST_SHIPPING FEE_ 04.02'
                         or trx.note = 'ADJUSTMENT_SHIPPING FEE_11/11/2021'
                         or trx.note LIKE '%ADJUSTMENT_SHIPPING FEE_12/12/2021%')  then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as other_adjustment
    ,SUM(case when trx.txn_type in (201,301,401,104,1000,2001,2101) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as shipping_fee_share
    ,SUM(case when trx.txn_type in (202,302,402,1001,2002,2102) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as return_fee_share
    ,SUM(case when trx.txn_type in (204,304,404,105,1003,2004,2104) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as additional_bonus
    ,SUM(case when trx.txn_type in (200,300,400,101,1006,2000,2100) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as order_completed_bonus
    ,SUM(case when trx.txn_type in (203,303,403,106,2003,2005,2006,2007,2105,2106,129,131,133,135,110) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as other_payables
    ,SUM(case when trx.txn_type in (112,115) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as bad_weather_fee_temp
    ,SUM(case when trx.txn_type in (112,115) and (raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as bad_weather_fee_temp_hub
    ,SUM(case when trx.txn_type in (112,115) and (raw.partner_type <> 12 OR coalesce(raw.driver_payment_policy,0) = 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as bad_weather_fee_temp_non_hub

    ,SUM(case when trx.txn_type in (119) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as late_night_fee_temp
    ,SUM(case when trx.txn_type in (119) and (raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as late_night_fee_temp_hub
    ,SUM(case when trx.txn_type in (119) and (raw.partner_type <> 12 OR coalesce(raw.driver_payment_policy,0) = 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as late_night_fee_temp_non_hub

    ,SUM(case when trx.txn_type in (117) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as holiday_fee_temp
    ,SUM(case when trx.txn_type in (117) and (raw.partner_type = 12 and coalesce(raw.driver_payment_policy,0) <> 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as holiday_fee_temp_hub
    ,SUM(case when trx.txn_type in (117) and (raw.partner_type <> 12 OR coalesce(raw.driver_payment_policy,0) = 3) then trx.balance + trx.deposit else null end)*1.0/(100*1.0) as holiday_fee_temp_non_hub
    -- raw data: order data --> total bill
    from    (  SELECT *
                ,case when temp.total_shipping_fee_collected_from_customer is null then 0
                        else 1 end as is_valid_for_calculating_shipping_fee_collected_from_customer

                from
                (SELECT o.order_id
                ,o.partner_id
                ,o.city_name
                ,o.city_id
                ,o.date_
                ,o.year_week
                ,o.partner_type
                ,case when o.source = 'Now Ship Shopee' then o.collect_from_customer
                    else o.total_shipping_fee end as total_shipping_fee_collected_from_customer
                ,o.source
                ,o.food_service
                ,coalesce(o.distance,0) as distance
                ,o.status
                ,coalesce(o.total_shipping_fee,0) as total_shipping_fee
                ,coalesce(o.total_shipping_fee_basic,0) as total_shipping_fee_basic
                ,coalesce(o.total_shipping_fee_surge,0) as total_shipping_fee_surge
                ,coalesce(o.total_return_fee,0) as total_return_fee
                ,coalesce(o.total_return_fee_basic,0) as total_return_fee_basic
                ,coalesce(o.total_return_fee_surge,0) as total_return_fee_surge
                ,coalesce(o.bad_weather_cost_driver_new,0) as bad_weather_cost_driver_new
                ,coalesce(o.user_bwf,0) as user_bwf
                -- revenue calculation
                ,coalesce(o.rev_shipping_fee,0) as rev_shipping_fee
                ,coalesce(o.prm_cost,0) as prm_cost
                ,coalesce(o.rev_cod_fee,0) as rev_cod_fee
                ,coalesce(o.rev_return_fee,0) as rev_return_fee
                ,o.driver_payment_policy

                from
                        (--EXPLAIN ANALYZE
                        -- Food / Market
                        select  distinct ad_odt.order_id,ad_odt.partner_id
                            ,case when ad_odt.city_id = 217 then 'HCM'
                                  when ad_odt.city_id = 218 then 'HN'
                                  when ad_odt.city_id = 219 then 'DN'
                                  when ad_odt.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_odt.city_id
                            ,cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 3600) as date) as date_
                            ,case when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                  when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600) as date) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                                    else YEAR(cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600) as date))*100 + WEEK(cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time) - 3600) as date)) end as year_week
                            ,ad_odt.partner_type
--                             ,case when oct.foody_service_id = 1 then 'Food'
--                                     else 'Market' end as source
--                             ,case when oct.foody_service_id = 1 then 'Food'
--                                     else 'Market' end as sub_source
                            ,'order_delivery' as source
                            ,case when oct.foody_service_id = 1 then 'Food'
                                  when oct.foody_service_id = 5 then 'Fresh'
                                    else 'Market' end as food_service
                            ,0 as collect_from_customer
                            ,oct.distance
                            ,oct.status
                            -- ,oct.total_shipping_fee*1.00/100 as total_shipping_fee
                            --,coalesce(cast(json_extract(oct.extra_data,'$.bad_weather_fee.user_pay_amount') as decimal),0) as user_bwf
                            ,oct.user_bwf
                            ,coalesce(dotet.total_shipping_fee,0) as total_shipping_fee

                            ,case when oct.status = 9 then dotet.total_shipping_fee
                                  when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4500)*oct.distance)  -- change setting
                                  else GREATEST(15000,coalesce(dotet.unit_fee,5000)*oct.distance)
                                  end as total_shipping_fee_basic


                            ,case when oct.status = 9 then 0
                                when cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 3600) as date) >= date('2021-02-01') then GREATEST( dotet.total_shipping_fee - coalesce(GREATEST(13500,coalesce(dotet.unit_fee,4500)*oct.distance),0) ,0)
                                else GREATEST(dotet.total_shipping_fee -  coalesce(GREATEST(15000,coalesce(dotet.unit_fee,5000)*oct.distance),0)   ,0)
                                end as total_shipping_fee_surge


                            ,case when dotet.total_shipping_fee = coalesce(dotet.min_fee,0) + coalesce(dotet.bwf_surge_min_fee,0)
                                    then coalesce(dotet.bwf_surge_min_fee,0)
                                    else coalesce(dotet.unit_fee,0)*oct.distance*coalesce(dotet.bwf_surge_rate,0)
                                    end as bad_weather_cost_driver_new

                            ,0 as total_return_fee
                            ,0 as total_return_fee_basic
                            ,0 as total_return_fee_surge

                            -- revenue calculation
                            ,0 as rev_shipping_fee
                            ,0 as prm_cost
                            ,0 as rev_cod_fee
                            ,0 as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                        from shopeefood.foody_accountant_db__order_delivery_tab__reg_daily_s0_live ad_odt
                        left join (SELECT id,submit_time,foody_service_id,distance,status,total_shipping_fee,extra_data
                                        ,coalesce(cast(json_extract(oct.extra_data,'$.bad_weather_fee.user_pay_amount') as decimal),0) as user_bwf

                                    from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct) oct on oct.id = ad_odt.order_id
                        left JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = oct.id and dot.ref_order_category = 0
                        left join (SELECT order_id
                                        ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                        ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                        ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                    from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                    )dotet on dot.id = dotet.order_id

                        where cast(from_unixtime(coalesce(ad_odt.delivered_date,ad_odt.create_time)  - 3600) as date) BETWEEN {{start_date}} - interval '2' day and {{end_date}} + interval '2' day
                        and ad_odt.partner_id > 0

                        union all

                      --  EXPLAIN ANALYZE
                        -- NS User = NS Instant
                        select  distinct ad_ns.order_id,ad_ns.partner_id --,dot.ref_order_code
                            ,case when ad_ns.city_id = 217 then 'HCM'
                                  when ad_ns.city_id = 218 then 'HN'
                                  when ad_ns.city_id = 219 then 'DN'
                                  when ad_ns.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_ns.city_id
                            ,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) as date_
                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                   when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                                    else YEAR(cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date))*100 + WEEK(cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date)) end as year_week
                            ,ad_ns.partner_type
--                             ,'Now Ship' as source
--                             ,'NS Instant' as sub_source
                            ,'now_ship_customer' as source
                            ,'NowShip' as food_service
                            ,0 as collect_from_customer
                            ,ebt.distance*1.00/1000 as distance
                            ,ebt.status
                            ,0 as user_bwf
                            ,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
                                end as total_shipping_fee_basic

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >=  date('2021-02-01') then
                                    GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))  ,0)
                                else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))   ,0)
                                end as total_shipping_fee_surge

                            ,0 as bad_weather_cost_driver_new
                            ,case when ebt.status in (14,19) then cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) -- returned
                                    else 0 end as total_return_fee
                            ,case when ebt.status in (14,19) then GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2
                                    else 0 end as total_return_fee_basic
                            ,case when ebt.status in (14,19) then GREATEST(coalesce(cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2,0),0)
                                    else 0 end as total_return_fee_surge
                           -- ,dot.ref_order_id
                            --,dot.ref_order_code

                            -- revenue calculation
                            ,eojd.delivery_cost_amount as rev_shipping_fee
                            ,case when prm.code LIKE '%NOW%' then eojd.foody_discount_amount
                  when prm.code LIKE '%NOWSHIP%' then eojd.foody_discount_amount
                                  else 0 end as prm_cost

                           -- , case when prm.code LIKE 'NOW%' and cast(json_extract(prm.conditions, '$.promotion_type') as DOUBLE) = 2 then 'ns_prm'
                            --       when prm.code LIKE 'NS%' and cast(json_extract(prm.conditions, '$.promotion_type') as DOUBLE) = 1 then 'e_voucher'
                            --       else null end as prm_type
                        --    , case when ebt.promotion_code_id = 0 then 'no promotion'
                          --          when prm.code LIKE 'NOW%'  then 'ns_prm'
                            --       when prm.code LIKE 'NS%'  then 'e_voucher'
                             --      else null end as prm_type_test

                            ,case when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
                                    else 0 end as rev_cod_fee
                            ,case when ebt.status = 14 then eojd.shipping_return_fee else 0 end as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


                        from shopeefood.foody_accountant_db__order_now_ship_user_tab__reg_daily_s0_live ad_ns
                        Left join shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live ebt on ebt.id = ad_ns.order_id
                        left join
                                 (SELECT id,create_timestamp,delivery_cost_amount,foody_discount_amount,shipping_return_fee
                                  FROM shopeefood.foody_mart__fact_express_order_join_detail

                                  WHERE grass_region = 'VN'
                                 )eojd on eojd.id = ebt.id
                        left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 4

                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                        )dotet on dot.id = dotet.order_id

                        left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id

                        where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) BETWEEN {{start_date}} - interval '2' day and {{end_date}} + interval '2' day
                        and ad_ns.partner_id > 0

                        union all

                    --    EXPLAIN ANALYZE
                        -- NS Merchant = NS Food Merchant
                        select  distinct ad_ns.order_id,ad_ns.partner_id
                            ,case when ad_ns.city_id = 217 then 'HCM'
                                  when ad_ns.city_id = 218 then 'HN'
                                  when ad_ns.city_id = 219 then 'DN'
                                  when ad_ns.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_ns.city_id
                            ,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) as date_
                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                   when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                                    else YEAR(cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date))*100 + WEEK(cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date)) end as year_week
                            ,ad_ns.partner_type
--                             ,'Now Ship' as source
--                             ,'NS Food Merchant' as sub_source
                            ,'now_ship_merchant' as source
                            ,'NowShip' as food_service
                            ,0 as collect_from_customer
                            ,ebt.distance*1.00/1000 as distance
                            ,ebt.status
                            ,0 as user_bwf
                            ,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
                                end as total_shipping_fee_basic

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >=  date('2021-02-01') then
                                    GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))  ,0)
                                else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))   ,0)
                                end as total_shipping_fee_surge

                            ,0 as bad_weather_cost_driver_new
                            ,case when ebt.status in (14,19) then cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) -- returned
                                    else 0 end as total_return_fee
                            ,case when ebt.status in (14,19) then GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2
                                    else 0 end as total_return_fee_basic
                            ,case when ebt.status in (14,19) then GREATEST(coalesce(cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2,0),0)
                                    else 0 end as total_return_fee_surge

                            -- revenue calculation
                            ,eojd.delivery_cost_amount as rev_shipping_fee
                            ,case when prm.code LIKE 'NOW%' then eojd.foody_discount_amount
                  when prm.code LIKE '%NOWSHIP%' then eojd.foody_discount_amount
                                  else 0 end as prm_cost

                            ,case when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
                                    else 0 end as rev_cod_fee
                            ,case when ebt.status = 14 then eojd.shipping_return_fee else 0 end as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


                        from shopeefood.foody_accountant_db__order_now_ship_merchant_tab__reg_daily_s0_live ad_ns
                        Left join shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live ebt on ebt.id = ad_ns.order_id
                        left join
                                 (SELECT id,create_timestamp,delivery_cost_amount,foody_discount_amount,shipping_return_fee
                                  FROM shopeefood.foody_mart__fact_express_order_join_detail

                                  WHERE grass_region = 'VN'
                                 )eojd on eojd.id = ebt.id
                        left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 5

                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                        )dotet on dot.id = dotet.order_id

                        left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id

                        where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) BETWEEN {{start_date}} - interval '2' day and {{end_date}} + interval '2' day
                        and ad_ns.partner_id > 0

                        union all

                    --    EXPLAIN ANALYZE
                        -- NS Shopee
                        select  distinct ad_nss.order_id,ad_nss.partner_id
                            ,case when ad_nss.city_id = 217 then 'HCM'
                                  when ad_nss.city_id = 218 then 'HN'
                                  when ad_nss.city_id = 219 then 'DN'
                                  when ad_nss.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_nss.city_id
                            ,cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date) as date_
                            ,case when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                                    else YEAR(cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date))*100 + WEEK(cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date)) end as year_week
                            ,ad_nss.partner_type
--                             ,'Now Ship Shopee' as source
--                             ,'Now Ship Shopee' as sub_source
                            ,'now_ship_shopee' as source
                            ,'NowShip' as food_service
                            ,0 as collect_from_customer
                            ,esbt.distance*1.00/1000 as distance
                            ,esbt.status
                            ,0 as user_bwf
                            ,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

                            ,case when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(esbt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(esbt.distance*1.00/1000))
                                end as total_shipping_fee_basic

                            ,case when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date) >=  date('2021-02-01') then
                                    GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(esbt.distance*1.00/1000))  ,0)
                                else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(esbt.distance*1.00/1000))   ,0)
                                end as total_shipping_fee_surge

                            ,0 as bad_weather_cost_driver_new
                            ,case when esbt.status in (14,19) then cast(json_extract(esbt.extra_data, '$.shipping_fee.return_fee') as DOUBLE)  -- returned
                                    else 0 end as total_return_fee
                            ,case when esbt.status in (14,19) then GREATEST(15000,5000*(esbt.distance*1.00/1000))*1.00/2
                                    else 0 end as total_return_fee_basic
                            ,case when esbt.status in (14,19) then GREATEST(coalesce(cast(json_extract(esbt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(esbt.distance*1.00/1000))*1.00/2,0),0)
                                    else 0 end as total_return_fee_surge

                            -- revenue calculation
                            ,case
                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2022-02-04') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 18654.84
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26508.6
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 29945.16
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 29945.16+ (ceiling(esbt.distance *1.000 / 1000) -6 )*4418.28
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 91801.08 + (ceiling(esbt.distance *1.000 / 1000) -20)*7854.84
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 18654.84*1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26508.6 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 29945.16*1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 20 then (29945.16 + (ceiling(esbt.distance *1.000 / 1000) -6)*4418.28) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (91801.08 + (ceiling(esbt.distance *1.000 / 1000) -20)*7854.84) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2022-01-30') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 29454.84
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 37308.6
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 40745.16
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 40745.16 + (ceiling(esbt.distance *1.000 / 1000) -6 )*4418.28
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 102601.08 + (ceiling(esbt.distance *1.000 / 1000) -20)*7854.84
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 29454.84 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 37308.6 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 40745.16*1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 20 then (40745.16 + (ceiling(esbt.distance *1.000 / 1000) -6)*4418.28) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (102601.08 + (ceiling(esbt.distance *1.000 / 1000) -20)*7854.84) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-10-13') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 19000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 27000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 30500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 30500 + (ceiling(esbt.distance *1.000 / 1000) -6 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 19000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 27000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 30500*1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 20 then (30500 + (ceiling(esbt.distance *1.000 / 1000) -6)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-10-01') and ad_nss.city_id = 217 then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 28000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 28000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 37000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 37000 + (ceiling(esbt.distance *1.000 / 1000) -6 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 100000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 28000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 28000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 6 then 37000*1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 20 then (37000 + (ceiling(esbt.distance *1.000 / 1000) -6)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (100000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-09-30') and ad_nss.city_id != 217 then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 26000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 26000 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 26000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (26000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-09-29') and ad_nss.city_id = 218 then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 26000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 26000 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 26000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (26000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-09-01') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 35000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 35000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 41500 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 109000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 35000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 35000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (41500 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (109000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-08-19') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 27000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 27000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 33500 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 101000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 27000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 27000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (33500 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (101000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-08-18') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 20000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 28000
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 26000 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                            when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 20000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 28000 *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (26000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                            when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                            else null
                                        end

                                    when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-07-15') then
                                        case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 18000
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 26000 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 18000 *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 26000 *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (26000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                                                else null
                                        end

                                       when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-03-01') then
                                                            case when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 17500
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 25500
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 20 then 25500 + (ceiling(esbt.distance *1.000 / 1000) -5 )*4500
                                                                when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 17500 *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 25500 *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (25500 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                                                when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (93000 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                                                else null
                                                            end
                                                        -- before 2021-03-01 - change NSS ratecard
                                                        when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 15000
                                                        when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 25000
                                                        when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) > 5 then 25000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500
                                                        when esbt.status = 11 and ceiling(esbt.distance *1.000 / 1000) >20 then 92500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000
                                                        when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000)  <= 3 then 15000 *1.5
                                                        when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) <= 5 then 25000 *1.5
                                                        when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) > 5 then (25000 + (ceiling(esbt.distance *1.000 / 1000) -5)*4500) *1.5
                                                        when esbt.status = 14 and ceiling(esbt.distance *1.000 / 1000) >20 then (92500 + (ceiling(esbt.distance *1.000 / 1000) -20)*8000) *1.5
                                                        else null end as rev_shipping_fee
                            ,0 as prm_cost
                            ,0 as rev_cod_fee
                            ,0 as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


                        from shopeefood.foody_accountant_db__order_now_ship_shopee_tab__reg_daily_s0_live ad_nss
                        Left join shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live esbt on esbt.id = ad_nss.order_id
                        left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on esbt.id = dot.ref_order_id and dot.ref_order_category = 6

                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                        )dotet on dot.id = dotet.order_id


                        where cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600) as date) BETWEEN {{start_date}} - interval '2' day and {{end_date}} + interval '2' day
                        and ad_nss.partner_id > 0
                        and booking_type = 4

                    UNION all
                       --    EXPLAIN ANALYZE
                        -- SPX Portal
                        select  distinct ad_nss.order_id,ad_nss.partner_id
                            ,case when ad_nss.city_id = 217 then 'HCM'
                                  when ad_nss.city_id = 218 then 'HN'
                                  when ad_nss.city_id = 219 then 'DN'
                                  when ad_nss.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_nss.city_id
                            ,cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) as date_
                            ,CASE
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) >= 52 AND MONTH(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))-1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))
                                WHEN WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) = 1 AND MONTH(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))+1)*100 + WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))
                            ELSE YEAR(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600)))*100 + WEEK(DATE(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 3600))) END as year_week
                            ,ad_nss.partner_type
                            ,'Now Ship' as source
                            ,'SPX Portal' as sub_source
                            ,0 as collect_from_customer
                            ,esbt.distance*1.00/1000 as distance
                            ,esbt.status
                            ,0 as user_bwf
                            ,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

                            ,case when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(esbt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(esbt.distance*1.00/1000))
                                end as total_shipping_fee_basic

                            ,case when cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >=  date('2021-02-01') then
                                    GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(esbt.distance*1.00/1000))  ,0)
                                else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(esbt.distance*1.00/1000))   ,0)
                                end as total_shipping_fee_surge

                            ,0 as bad_weather_cost_driver_new
                            ,case when esbt.status in (14,19) then cast(json_extract(esbt.extra_data, '$.shipping_fee.return_fee') as DOUBLE)  -- returned
                                    else 0 end as total_return_fee
                            ,case when esbt.status in (14,19) then GREATEST(15000,5000*(esbt.distance*1.00/1000))*1.00/2
                                    else 0 end as total_return_fee_basic
                            ,case when esbt.status in (14,19) then GREATEST(coalesce(cast(json_extract(esbt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(esbt.distance*1.00/1000))*1.00/2,0),0)
                                    else 0 end as total_return_fee_surge

                            -- revenue calculation
                            ,cast(json_extract(esbt.extra_data, '$.shipping_fee.shipping_fee_origin') as DOUBLE) as rev_shipping_fee
                            ,0 as prm_cost
                            ,0 as rev_cod_fee
                            ,case when status in (14,15,22) then cast(json_extract(esbt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) else 0 end as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


                        from shopeefood.foody_accountant_db__order_now_ship_shopee_tab__reg_daily_s0_live ad_nss
                        Left join shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live esbt on esbt.id = ad_nss.order_id and esbt.create_time > 1609439493
                        left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on esbt.id = dot.ref_order_id and dot.ref_order_category = 6 and dot.submitted_time > 1609439493

                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                        )dotet on dot.id = dotet.order_id


                        where cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) >=  date('2020-12-31') -- ate(current_date) - interval '75' day
                        and cast(from_unixtime(coalesce(ad_nss.delivered_date,ad_nss.create_time) - 60*60) as date) <= date(current_date)
                        and ad_nss.partner_id > 0
                        and booking_type = 5

                    UNION all

                --    EXPLAIN ANALYZE
                    -- NS Same Day
                        SELECT distinct ad_ns.order_id,ad_ns.partner_id
                            ,case when ad_ns.city_id = 217 then 'HCM'
                                  when ad_ns.city_id = 218 then 'HN'
                                  when ad_ns.city_id = 219 then 'DN'
                                  when ad_ns.city_id = 220 then 'HP'
                                  else 'OTH' end as city_name
                            ,ad_ns.city_id
                            ,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) as date_
                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                   when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                                    else YEAR(cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date))*100 + WEEK(cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date)) end as year_week
                            ,ad_ns.partner_type
--                             ,'Now Ship' as source
--                             ,'NS Sameday' as sub_source
                            ,'now_ship_sameday' as source
                            ,'NowShip' as food_service
                            ,0 as collect_from_customer
                            ,ebt.distance*1.00/1000 as distance
                            ,ebt.status
                            ,0 as user_bwf
                            ,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
                                else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
                                end as total_shipping_fee_basic

                            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >=  date('2021-02-01') then
                                    GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))  ,0)
                                else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))   ,0)
                                end as total_shipping_fee_surge

                            ,0 as bad_weather_cost_driver_new
                            ,case when ebt.status in (14,19) then cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) -- returned
                                    else 0 end as total_return_fee
                            ,case when ebt.status in (14,19) then GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2
                                    else 0 end as total_return_fee_basic
                            ,case when ebt.status in (14,19) then GREATEST(coalesce(cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2,0),0)
                                    else 0 end as total_return_fee_surge

                            -- revenue calculation
                            ,eojd.delivery_cost_amount as rev_shipping_fee
                            ,case when prm.code LIKE 'NOW%' then eojd.foody_discount_amount
                  when prm.code LIKE '%NOWSHIP%' then eojd.foody_discount_amount
                                  else 0 end as prm_cost

                            ,case when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
                                    else 0 end as rev_cod_fee
                            ,case when ebt.status = 14 then eojd.shipping_return_fee else 0 end as rev_return_fee

                            -- hub order
                            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


                        from shopeefood.foody_accountant_db__order_now_ship_sameday_tab__reg_daily_s0_live ad_ns
                        Left join shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live ebt on ebt.id = ad_ns.order_id
                        left join
                                 (SELECT id,create_timestamp,delivery_cost_amount,foody_discount_amount,shipping_return_fee
                                  FROM shopeefood.foody_mart__fact_express_order_join_detail

                                  WHERE grass_region = 'VN'
                                 )eojd on eojd.id = ebt.id
                        left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 7 and dot.submitted_time > 1609439493

                        left join (SELECT order_id
                                            ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                                            ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                                            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                                        from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                                        )dotet on dot.id = dotet.order_id

                        left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id

                        where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) BETWEEN {{start_date}} - interval '2' day and {{end_date}} + interval '2' day
                        and ad_ns.partner_id > 0

                    --    limit 100

          UNION all

                    -- NS Multi Drop

                    select  distinct ad_ns.order_id,ad_ns.partner_id -- ,dot.ref_order_code,ebt.id  as ebt_id, eojd.id  as eojd_id
            ,case when ad_ns.city_id = 217 then 'HCM'
                when ad_ns.city_id = 218 then 'HN'
                when ad_ns.city_id = 219 then 'DN'
                when ad_ns.city_id = 220 then 'HP'
                else 'OTH' end as city_name
            ,ad_ns.city_id
            ,cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) as date_
            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                 when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                else YEAR(cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date))*100 + WEEK(cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date)) end as year_week
            ,ad_ns.partner_type
--            ,'Now Ship' as source
--            ,'NS Instant' as sub_source
                        ,'now_ship_multi_drop' as source
                        ,'NowShip' as food_service
            ,0 as collect_from_customer
            ,ebt.distance*1.00/1000 as distance
            ,ebt.status
            ,0 as user_bwf
            ,dot.delivery_cost*1.00000000/100 as total_shipping_fee  -- ok

            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >= date('2021-02-01') then GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))
              else GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))
              end as total_shipping_fee_basic

            ,case when cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) >=  date('2021-02-01') then
                GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(13500,coalesce(dotet.unit_fee,4050)*(ebt.distance*1.00/1000))  ,0)
              else GREATEST(dot.delivery_cost*1.00000000/100 - GREATEST(15000,coalesce(dotet.unit_fee,5000)*(ebt.distance*1.00/1000))   ,0)
              end as total_shipping_fee_surge

            ,0 as bad_weather_cost_driver_new
            ,case when ebt.status in (14,19) then cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) -- returned
                else 0 end as total_return_fee
            ,case when ebt.status in (14,19) then GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2
                else 0 end as total_return_fee_basic
            ,case when ebt.status in (14,19) then GREATEST(coalesce(cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE),0) - coalesce(GREATEST(15000,5000*(ebt.distance*1.00/1000))*1.00/2,0),0)
                else 0 end as total_return_fee_surge
             -- ,dot.ref_order_id
            --,dot.ref_order_code

            -- revenue calculation
             -- ,eojd.delivery_cost_amount as rev_shipping_fee
            ,coalesce(cast(json_extract(ebt.extra_data, '$.shipping_fee.shipping_fee_origin') as DOUBLE),0)
              + coalesce(case
                when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 10 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
                when cast(json_extract(ebt.extra_data, '$.other_fees[1].other_fee_type') as DOUBLE) = 10 then cast(json_extract(ebt.extra_data, '$.other_fees[1].value') as DOUBLE)
                when cast(json_extract(ebt.extra_data, '$.other_fees[2].other_fee_type') as DOUBLE) = 10 then cast(json_extract(ebt.extra_data, '$.other_fees[2].value') as DOUBLE)
                else 0 end,0)-- as rev_drop_fee
              as rev_shipping_fee


            ,case when prm.code LIKE '%NOW%' then ebt.discount_amount
                when prm.code LIKE '%NOWSHIP%' then ebt.discount_amount
                              when prm.code LIKE '%SPXINSTANT%' then ebt.discount_amount
                else 0 end as prm_cost
             -- ,case when prm.code like '%NOW%' then ebt.discount_amount else 0 end as prm_cost


             -- , case when prm.code LIKE 'NOW%' and cast(json_extract(prm.conditions, '$.promotion_type') as DOUBLE) = 2 then 'ns_prm'
            --       when prm.code LIKE 'NS%' and cast(json_extract(prm.conditions, '$.promotion_type') as DOUBLE) = 1 then 'e_voucher'
            --       else null end as prm_type
          --    , case when ebt.promotion_code_id = 0 then 'no promotion'
            --          when prm.code LIKE 'NOW%'  then 'ns_prm'
            --       when prm.code LIKE 'NS%'  then 'e_voucher'
             --      else null end as prm_type_test

             ,case
              when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
              when cast(json_extract(ebt.extra_data, '$.other_fees[1].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[1].value') as DOUBLE)
              when cast(json_extract(ebt.extra_data, '$.other_fees[2].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[2].value') as DOUBLE)
              else 0 end as rev_cod_fee
            --  ,case when cast(json_extract(ebt.extra_data, '$.other_fees[0].other_fee_type') as DOUBLE) = 6 then cast(json_extract(ebt.extra_data, '$.other_fees[0].value') as DOUBLE)
          --            else 0 end as rev_cod_fee
          --    ,case when ebt.status = 14 then eojd.shipping_return_fee else 0 end as rev_return_fee
            ,case when ebt.status = 14 then cast(json_extract(ebt.extra_data, '$.shipping_fee.return_fee') as DOUBLE) else 0 end as rev_return_fee

            -- hub order
            ,coalesce(dotet.driver_payment_policy,1) as driver_payment_policy  -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)


          from shopeefood.foody_accountant_db__order_now_ship_multi_drop_tab__reg_daily_s0_live ad_ns
          Left join shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live ebt on ebt.id = ad_ns.order_id

          left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on ebt.id = dot.ref_order_id and dot.ref_order_category = 8

          left join (SELECT order_id
                    ,cast(json_extract(dotet.order_data,'$.delivery.shipping_fee.total') as double) as total_shipping_fee
                    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.unit_fee') as double) as unit_fee
                    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.min_fee') as double) as min_fee
                    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_min_fee') as double) as bwf_surge_min_fee
                    ,cast(json_extract(dotet.order_data,'$.shipping_fee_config.bad_weather_surge.surge_rate') as double) as bwf_surge_rate
                    ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy -- driver_payment_policy: 1/blank: normal, 2: hub in shift (hub pricing), 3: hub out shift (normal pricing)

                  from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet

                  )dotet on dot.id = dotet.order_id

          left join shopeefood.foody_express_db__promotion_tab__reg_daily_s0_live prm on ebt.promotion_code_id = prm.id

          where cast(from_unixtime(coalesce(ad_ns.delivered_date,ad_ns.create_time) - 3600) as date) BETWEEN {{start_date}} - interval '2' day and {{end_date}} + interval '2' day
          and ad_ns.partner_id > 0
          -- and dot.ref_order_code = '210709SE2955'

          --limit 1000

                    )o
                )temp
            )raw

    -- transaction tbl --> calculate fee
    left join (SELECT reference_id
                    ,txn_type
                    ,balance
                    ,deposit
                    ,case when cast(from_unixtime(create_time,7,0) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                            when cast(from_unixtime(create_time,7,0) as date) between DATE('2020-12-28') and DATE('2021-01-03') then 202053
                            else YEAR(cast(from_unixtime(create_time,7,0) as date))*100 + WEEK(cast(from_unixtime(create_time,7,0) as date)) end as year_week
                    ,date(from_unixtime(create_time - 3600)) as created_date
                    ,user_id
                    ,note
                    ,create_time

                from shopeefood.foody_accountant_db__partner_transaction_tab__reg_daily_s0_live

                where date(from_unixtime(create_time - 3600)) between {{start_date}} - interval '2' day and {{end_date}} + interval '2' day
                -- and cast(from_unixtime(create_time,7,0) as date) >= date('2019-12-01') -- date(current_date) - interval '78' day
                -- and cast(from_unixtime(create_time,7,0) as date) <= date(current_date)

                and txn_type in (-- TYPE: BONUS, RECEIVED SHIPPING FEE, ADDITIONAL BONUS, OTHER PAYABLES (parking fee), RETURN FEE SHARED
                                          200,201,204,203,202, -- Now Ship User
                                          300,301,304,303,302, -- Now Ship Merchant
                                          400,401,404,403,402, -- Now Moto

                                          101,104,105,106,129,131,133,135,110,      -- Delivery Service, consider 105 DELIVERY_ADD_BONUS_MANUAL, 129:DELIVERY_ADD_HAND_DELIVERY_FEE_PASSTHROUGH, 131: DELIVERY_ADD_PARKING_FEE_PASSTHROUGH
                                                                                                                                        --133: DELIVERY_ADD_MERCHANT_PARKING_FEE_PASSTHROUGH, 135: DELIVERY_ADD_TIP_FEE_PASSTHROUGH
                                                                                                                                        --110: DELIVERY_ADD_AFTER_DELIVERY_TIP_FEE

                                          1006,1000,1003,1001,  -- Now Ship Shopee: 1000: recevied shipping fee, 1001: return fee shared, 1003: bonus from CS, 1006: bonus for FT driver
                                          2000,2001,2004,2003,2002,2005,2006,2007, -- Sameday
                      2100,2101,2104,2105,2106,2102, -- multidrop
                                          112,115, -- bad weather fee
                                          119, -- late night fee
                                          117, -- holiday fee
                                          906,907, -- hub auto
                                          501,518,512,560,900,901, -- hub adjustment
                                          565 -- 2021.02.04 adjustment
                                )
            --    and reference_id = 182853798 -- 183461946

                )trx on trx.reference_id = raw.order_id
                    and trx.user_id = raw.partner_id -- user_id = partner_id = shipper_id
                    and trx.created_date >= raw.date_ - interval '2' day and trx.created_date <= raw.date_ + interval '2' day      -- map by order Id --> more details than shipper_id



    GROUP   BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
    )raw2
 --   where raw2.order_id = 123030544
 --    and raw2.city_name = 'HCM'
 --   and raw2.source = 'now_ship_user'
   -- and raw2.status in (9)
-- limit 1000
) bill_fee on base.order_id = bill_fee.order_id and base.source = bill_fee.source and base.shipper_id = bill_fee.partner_id

LEFT JOIN -- hub shift
    (
    SELECT
        shipper_id
        ,shipper_type_id
        ,report_date
        ,shipper_shift_id
        ,start_shift
        ,end_shift
        ,off_weekdays
        ,registration_status
        ,if(report_date < DATE'2021-10-22' and off_date is null and registration_status is null, off_date_1, off_date) as off_date
    FROM
        (
        SELECT  sm.shipper_id
                ,sm.shipper_type_id
                ,try_cast(sm.grass_date as date) as report_date
                ,sm.shipper_shift_id
                ,case
                    when try_cast(sm.grass_date as date) < date'2021-10-22' then
                        case
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                  ) then if((ss2.end_time - ss2.start_time)*1.00/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.00/3600 < 10.00, (ss2.end_time - 28800)/3600, ss2.start_time/3600)
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                  ) then null
                            else if((ss1.end_time - ss1.start_time)*1.00/3600 > 5.00 and (ss1.end_time - ss1.start_time)*1.00/3600 < 10.00, (ss1.end_time - 28800)/3600, ss1.start_time/3600)
                        end
                    else
                        if(ss2.end_time is not null, if((ss2.end_time - ss2.start_time)*1.00/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.00/3600 < 10.00, (ss2.end_time - 28800)/3600, ss2.start_time/3600)
                            ,if((ss1.end_time - ss1.start_time)*1.00/3600 > 5.00 and (ss1.end_time - ss1.start_time)*1.00/3600 < 10.00, (ss1.end_time - 28800)/3600, ss1.start_time/3600)
                        )
                end as start_shift
                ,case
                    when try_cast(sm.grass_date as date) < date'2021-10-22' then
                        case
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                  ) then ss2.end_time/3600
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                  ) then null
                            else ss1.end_time/3600
                        end
                    else
                        if(ss2.end_time is not null, ss2.end_time/3600, ss1.end_time/3600)
                end as end_shift
                ,case
                    when ss2.registration_status = 1 then 'Registered'
                    when ss2.registration_status = 2 then 'Off'
                    when ss2.registration_status = 3 then 'Work'
                else
                    case
                        when try_cast(sm.grass_date as date) < date'2021-10-22' then null
                    else 'Off' end
                end as registration_status
                ,case
                    when try_cast(sm.grass_date as date) < date'2021-10-22' then
                        case
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                  ) then if(ss2.registration_status = 2, case
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                        end, null)
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                  ) then case
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                        end
                            else ss1.off_weekdays
                        end
                    else
                        if(ss2.end_time is not null, if(ss2.registration_status = 2, case
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                        end, null), case
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                            when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                        end)
                end as off_weekdays
                ,case
                    when try_cast(sm.grass_date as date) < date'2021-10-22' then
                        case
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                  ) then if(ss2.registration_status = 2, date_format(try_cast(sm.grass_date as date), '%a'), null)
                            when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                  ) then date_format(try_cast(sm.grass_date as date), '%a')
                            else null
                        end
                    else
                        if(ss2.end_time is not null, if(ss2.registration_status = 2, date_format(try_cast(sm.grass_date as date), '%a'), null)
                        , date_format(try_cast(sm.grass_date as date), '%a'))
                end as off_date
                ,array_join(array_agg(cast(d_.cha_date as VARCHAR)),', ') as off_date_1

                from shopeefood.foody_mart__profile_shipper_master sm
                left join shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss1 on ss1.id = sm.shipper_shift_id
                left join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live ss2 on ss2.uid = sm.shipper_id and date(from_unixtime(ss2.date_ts-3600)) = try_cast(sm.grass_date as date)
                left join
                         (SELECT
                                 case when off_weekdays = '1' then 'Mon'
                                      when off_weekdays = '2' then 'Tue'
                                      when off_weekdays = '3' then 'Wed'
                                      when off_weekdays = '4' then 'Thu'
                                      when off_weekdays = '5' then 'Fri'
                                      when off_weekdays = '6' then 'Sat'
                                      when off_weekdays = '7' then 'Sun'
                                      else 'No off date' end as cha_date
                                 ,off_weekdays as num_date

                          FROM shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live
                          WHERE 1=1
                          and off_weekdays in ('1','2','3','4','5','6','7')
                          GROUP BY 1,2
                         )d_ on regexp_like(ss1.off_weekdays,cast(d_.num_date  as varchar)) = true

                where 1=1
                and sm.grass_region = 'VN'
                and sm.grass_date != 'current'
                GROUP BY 1,2,3,4,5,6,7,8,9
        )
    ) driver_type on driver_type.shipper_id = base.shipper_id and driver_type.report_date = date(base.created_time)

LEFT JOIN vnfdbi_opsndrivers.snp_foody_hub_driver_mapping_tab d ON base.shipper_id = d.shipper_id AND date(base.created_time) = d.report_date
)

-- update hub3 2022.03.18
-- 2022.03.22 -- update logic hub_type 3 end shift - start_shift  = 4 (checkin time from 17h55)