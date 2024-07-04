SELECT
    report_date
    , report_hour
    , city_name
    , foody_service
    , is_CA
    , COUNT(DISTINCT shipper_id) AS active_drivers
    , SUM(gross_orders) AS gross_orders
    , SUM(net_orders) AS net_orders
    , SUM(canceled_orders) AS canceled_orders
    , SUM(cancel_no_driver_orders) AS cancel_no_driver_orders
    , TRY(CAST(SUM(net_orders) AS DOUBLE) / COUNT(DISTINCT shipper_id)) AS driver_ado
    , TRY(CAST(SUM(cancel_no_driver_orders) AS DOUBLE) / SUM(gross_orders)) AS canel_no_driver
FROM
(
-- NowShip
(SELECT
    created_date AS report_date
    , hour(created_timestamp) AS report_hour
    , city_name
    , IF(order_status in ('Delivered'), shipper_id, NULL) AS shipper_id
    , IF(is_auto_accepted_continuous_assign = 1, 1, 0) AS is_CA
    , IF(source = 'now_ship_shopee', 'NowShip On Shopee', 'NowShip Off Shopee') as foody_service
    , COUNT(DISTINCT uid) as gross_orders
    , COUNT(DISTINCT if(order_status in ('Delivered'), uid, null)) as net_orders
    , COUNT(DISTINCT if(order_status not in ('Delivered','Returned'), uid, null)) as canceled_orders
    , COUNT(DISTINCT if(is_no_driver_assign = 1, uid, null)) as cancel_no_driver_orders
FROM
(
SELECT base.*
      ,auto.is_auto_accepted
      ,auto.is_auto_accepted_continuous_assign
      ,dot.group_id
      ,dot.order_create_time
      ,dot.group_create_time
      ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and dot.order_create_time = dot.group_create_time then 1 else 0 end as is_group_order
      ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 1
            when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and dot.order_create_time != dot.group_create_time then 1
            else 0 end as  is_stacked
      ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and dot.order_create_time = dot.group_create_time then COALESCE(order_rank.total_order_in_group_original,0) else 0 end as total_order_in_group_original
      ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then COALESCE(order_rank.total_order_in_group,0)
            when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and dot.order_create_time != dot.group_create_time then COALESCE(order_rank.total_order_in_group,0) - COALESCE(order_rank.total_order_in_group_original,0)
            else 0 end as total_order_in_group

      ,coalesce(group_order.order_type,0) group_order_type
    --  ,coalesce(order_rank.total_order_in_group,0) total_order_in_group
   --   ,coalesce(order_rank.total_order_in_group_original,0) total_order_in_group_original
      ,case when base.report_date between date('2021-07-09') and date('2021-09-15') and base.city_id = 217 and is_hub_driver = 1 then 1
            when base.report_date between date('2021-07-24') and date('2021-09-06') and base.city_id = 218 and is_hub_driver = 1 then 1
            when cast(json_extract(dot.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_order_in_hub_shift
      ,cast(json_extract(dot.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
      ,district.name_en as district_name
      ,case when base.order_status = 'Delivered' then 1 else 0 end as is_del
      ,case when base.order_status = 'Cancelled' then 1 else 0 end as is_cancel
      ,case when base.order_status = 'Pickup Failed' then 1 else 0 end as is_pick_failed
      ,case when base.order_status = 'Returned' then 1 else 0 end as is_return
      ,case when base.order_status = 'Assigning Timeout' then 1 else 0 end as is_assign_timeout
      ,case when source = 'now_ship_shopee' then base.created_timestamp else assign.first_auto_assign_timestamp end as first_auto_assign_timestamp
      ,assign.last_incharge_timestamp

      ,date_diff('second',assign.last_incharge_timestamp,base.picked_timestamp)*1.0000/60 lt_pickup
      ,date_diff('second',base.created_timestamp,assign.last_incharge_timestamp)*1.0000/60 as lt_incharge

      ,case when (base.created_timestamp <= assign.last_incharge_timestamp) and base.created_timestamp is not null then 1 else 0 end as is_valid_lt_incharge
      ,case when (assign.last_incharge_timestamp <= base.picked_timestamp) and assign.last_incharge_timestamp is not null then 1 else 0 end as is_valid_lt_pickup
      ,case when base.is_valid_lt_deliver = 1 and base.delivered_timestamp > base.estimated_delivered_time then 1 else 0 end as is_late_delivered
      ,case when base.is_valid_lt_return = 1 and base.returned_timestamp > base.estimated_returned_time then 1 else 0 end as is_late_returned

      ,case when source = 'now_ship_shopee' then case when base.order_status = 'Assigning Timeout' then 1 else 0 end
            when source in ('now_ship_user','now_ship_merchant') then case when assign.last_incharge_timestamp is null and base.assigning_count > 0 and cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') then 1 else 0 end
            when source in ('now_ship_same_day') then case when assign.last_incharge_timestamp is null and cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') then 1 else 0 end
            else 0 end as is_no_driver_assign

      ,case when source = 'now_ship_shopee' and base.order_status = 'Assigning Timeout' then greatest(coalesce(assign.no_assign,0)+1,1)
            else greatest(base.assigning_count, coalesce(assign.no_assign,0)) end as actual_assigning_count

      ,coalesce(assign.no_incharged,0) no_incharged
      ,coalesce(assign.no_ignored,0) no_ignored
      ,coalesce(assign.no_deny,0) no_deny
      ,coalesce(assign.no_shipper_checkout,0) no_shipper_checkout
      ,coalesce(assign.no_incharge_error,0) no_incharge_error
      ,coalesce(assign.no_system_error,0) no_system_error

      ,case when dot.drop_city_id = 238 THEN 'Dien Bien' else drop_city.name_en end as drop_city_name
      ,drop_district.name_en as drop_district_name
FROM
        (

        --************** Now Ship/NSS
        SELECT ns.id
        ,ns.uid
        ,ns.shipper_id
        ,ns.code as order_code
        ,ns.shopee_order_code
        ,ns.customer_id
        -- time
        ,from_unixtime(ns.create_time - 3600) as created_timestamp
        ,nsc.created_timestamp as canceled_timestamp
        ,cast(from_unixtime(ns.create_time - 3600) as date) as created_date
        ,case  when cast(from_unixtime(ns.create_time - 3600) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
                when cast(from_unixtime(ns.create_time - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                when cast(from_unixtime(ns.create_time - 3600) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                else YEAR(cast(from_unixtime(ns.create_time - 3600) as date))*100 + WEEK(cast(from_unixtime(ns.create_time - 3600) as date)) end as created_year_week

        ,case when status in (11) and ns.drop_real_time > 0 then date(from_unixtime(ns.drop_real_time - 3600))
              when status in (14) and ns.update_time > 0 then date(from_unixtime(ns.update_time - 3600))
              else date(from_unixtime(ns.create_time- 3600)) end as report_date

        ,ns.distance*1.00/1000 as distance
        ,case when ns.distance*1.00/1000 <= 3 then '1. 0-3km'
            when ns.distance*1.00/1000 <= 4 then '2. 3-4km'
            when ns.distance*1.00/1000 <= 5 then '3. 4-5km'
            when ns.distance*1.00/1000 <= 7 then '4. 5-7km'
            when ns.distance*1.00/1000 > 7 then '5. 7km+'
            else null end as distance_range

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
        ,case when ns.status = 6 then 'USER_CANCELLED'
              when ns.status = 9 then 'DRIVER_CANCELLED'
              when ns.status = 12 then 'SYSTEM_CANCELLED'
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

        ,case when ns.status in (6,9,12) then
                  case when ns.booking_type <> 4 and ns.status = 12 and nsc.cancel_type is null then 'SYSTEM_CANCELLED'
                       when ns.booking_type = 4 and nsc.cancel_type is null then 'SYSTEM_CANCELLED'
                       else nsc.cancel_type end
              else null end as cancel_type

        ,case when ns.status in (6,9,12) then coalesce(nsc.cancel_reason,'No Reason') else null end as cancel_reason
        ,case when ns.status = 17 and ns.booking_type = 4 then pf_reason.pick_failed_reason else null end as pick_failed_reason
        ,date_diff('second',from_unixtime(ns.create_time - 3600),nsc.created_timestamp)*1.0000/60 as lt_submit_to_cancel
        ,case when ns.drop_real_time = 0 then null else date_diff('second',from_unixtime(ns.pick_real_time - 3600),from_unixtime(ns.drop_real_time - 3600))*1.0000/60 end as lt_deliver
        ,case when ns.status = 14 then date_diff('second',from_unixtime(ns.pick_real_time - 3600),from_unixtime(ns.update_time - 3600))*1.0000/60 else 0 end as lt_return
        ,case when ns.drop_real_time = 0 then null else date_diff('second',from_unixtime(ns.create_time - 3600),from_unixtime(ns.drop_real_time - 3600))*1.0000/60 end as lt_e2e

        ,from_unixtime(ns.pick_real_time - 3600) picked_timestamp
        ,case when ns.drop_real_time = 0 then NULL else from_unixtime(ns.drop_real_time - 3600) end as delivered_timestamp
        ,case when ns.status = 14 then from_unixtime(ns.update_time - 3600) else null end as returned_timestamp

        ,case when from_unixtime(ns.create_time - 3600) <= nsc.created_timestamp then 1 else 0 end as is_valid_lt_submit_to_cancel
        ,case when ns.status = 11 and from_unixtime(ns.pick_real_time - 3600) <= from_unixtime(ns.drop_real_time - 3600) and ns.drop_real_time > 0 and ns.pick_real_time > 0 then 1 else 0 end is_valid_lt_deliver
        ,case when ns.status = 14 and from_unixtime(ns.pick_real_time - 3600) <= from_unixtime(ns.update_time - 3600) and ns.pick_real_time > 0 and ns.update_time > 0 then 1 else 0 end as is_valid_lt_return
        ,case when from_unixtime(ns.create_time - 3600) <= from_unixtime(ns.drop_real_time - 3600) and ns.drop_real_time > 0 then 1 else 0 end as is_valid_lt_e2e


        ,case when ns.pick_real_time > 0 and ns.drop_real_time > 0 then
             case when ns.booking_type = 2 and ns.booking_service_type = 2 then date_add('minute',240,from_unixtime(ns.create_time - 3600))

             else
                  case when ns.distance <= 5000 then date_add('minute',40,from_unixtime(ns.pick_real_time - 3600)) -- as estimated_delivered_time
                       when ns.distance <= 10000 then date_add('minute',60,from_unixtime(ns.pick_real_time - 3600))
                       when ns.distance <= 15000 then date_add('minute',90,from_unixtime(ns.pick_real_time - 3600))
                       when ns.distance <= 20000 then date_add('minute',120,from_unixtime(ns.pick_real_time - 3600))
                       when ns.distance <= 25000 then date_add('minute',150,from_unixtime(ns.pick_real_time - 3600))
                       when ns.distance <= 30000 then date_add('minute',180,from_unixtime(ns.pick_real_time - 3600))
                       when ns.distance <= 180000 then date_add('minute',250,from_unixtime(ns.pick_real_time - 3600))
                       else null end
             end
            else null end as estimated_delivered_time

        ,case when ns.status = 14 and ns.pick_real_time > 0 and ns.update_time > 0 then
              case when ns.distance <= 5000 then date_add('minute',80,from_unixtime(ns.pick_real_time - 3600)) -- as estimated_delivered_time
                   when ns.distance <= 10000 then date_add('minute',120,from_unixtime(ns.pick_real_time - 3600))
                   when ns.distance <= 15000 then date_add('minute',200,from_unixtime(ns.pick_real_time - 3600))
                   when ns.distance <= 20000 then date_add('minute',260,from_unixtime(ns.pick_real_time - 3600))
                   when ns.distance <= 25000 then date_add('minute',300,from_unixtime(ns.pick_real_time - 3600))
                   when ns.distance <= 30000 then date_add('minute',360,from_unixtime(ns.pick_real_time - 3600))
                   when ns.distance <= 180000 then date_add('minute',500,from_unixtime(ns.pick_real_time - 3600))
                   else null end
            else null end as estimated_returned_time
        ,ns.assigning_count

        ,ns.sender_name
        ,ns.sender_phone
        ,ns.sender_address

        ,ns.receiver_name
        ,ns.receiver_phone
        ,ns.receiver_address
        ,case when driver_hub.shipper_type_id = 12 then 1 else 0 end as is_hub_driver

        from
                (SELECT id,concat('now_ship_',cast(id as VARCHAR)) as uid, code, booking_type, case when booking_type = 3 then cast(referal_id as varchar) else cast(customer_id as varchar) end as customer_id, shipper_id, distance,create_time, status, payment_method,'now_ship' as original_source,city_id,cast(json_extract(extra_data,'$.pick_address_info.district_id') as DOUBLE) as district_id
                        ,booking_service_type, pick_real_time, drop_real_time, pick_type, update_time, '' as shopee_order_code,assigning_count
                          ,cast(json_extract(extra_data, '$.pick_address_info.address') as varchar) as sender_address
                          ,cast(json_extract(extra_data, '$.sender_info.name')as varchar) as sender_name
                          ,cast(json_extract(extra_data, '$.sender_info.phone')as varchar) as sender_phone

                          ,cast(json_extract(extra_data, '$.drop_address_info.address') as varchar) as receiver_address
                          ,cast(json_extract(extra_data, '$.receiver_info.name')as varchar) as receiver_name
                          ,cast(json_extract(extra_data, '$.receiver_info.phone')as varchar) as receiver_phone


                    from shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live

                UNION

                SELECT id,concat('now_ship_shopee_',cast(id as VARCHAR)) as uid, code, 4 as booking_type, sender_username_v2 as customer_id, shipper_id,distance,create_time,status,1 as payment_method,'now_ship_shopee' as original_source,city_id,cast(json_extract(extra_data,'$.sender_info.district_id') as DOUBLE) as district_id
                        ,booking_service_type, pick_real_time, drop_real_time, 1 as pick_type, update_time, shopee_order_code, coalesce(a.assign_cnt,0) as assigning_count
                          ,cast(json_extract(extra_data, '$.sender_info.address') as varchar) as sender_address
                          ,cast(json_extract(extra_data, '$.sender_info.name')as varchar) as sender_name
                          ,cast(json_extract(extra_data, '$.sender_info.phone')as varchar) as sender_phone

                          ,cast(json_extract(extra_data, '$.recipient_info.address') as varchar) as receiver_address
                          ,cast(json_extract(extra_data, '$.recipient_info.name')as varchar) as receiver_name
                          ,cast(json_extract(extra_data, '$.recipient_info.phone')as varchar) as receiver_phone

                    from shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live sbt

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
                                      ,case when bc.cancel_comment != '' then bc.cancel_comment else COALESCE(cast(json_extract(bc.extra_data,'$.reasons[0].reason_content') as varchar), cast(json_extract(bc.extra_data,'$.cancel_reasons[0]') as varchar)) end as cancel_detail
                                      ,case when bc.booking_type = 4 then cast(json_extract(bc.extra_data, '$.reasons[0].reason_content') as varchar)
                                            when bc.booking_type != 4 then cast(json_extract(bc.extra_data,'$.cancel_reasons[0]') as varchar) else null end as cancel_reason


                                FROM shopeefood.foody_express_db__booking_cancel_tab__reg_daily_s0_live bc
                                )nsc
                    )nsc on nsc.booking_id = ns.id and nsc.booking_type = ns.booking_type and nsc.row_num = 1

                    LEFT JOIN
                            (SELECT bc.booking_id
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
                            )pf_reason on pf_reason.booking_id = ns.id and pf_reason.booking_type = ns.booking_type and pf_reason.row_num = 1

                    --- find whether driver is hub driver or not
                    LEFT JOIN
                            (
                             SELECT  sm.shipper_id
                                    ,sm.shipper_type_id
                                    ,case when sm.grass_date = 'current' then date(current_date)
                                        else cast(sm.grass_date as date) end as report_date

                                    from shopeefood.foody_mart__profile_shipper_master sm

                                    where 1=1
                                    and shipper_type_id <> 3
                                    and shipper_status_code = 1
                                    and grass_region = 'VN'
                                    GROUP BY 1,2,3
                            )driver_hub on driver_hub.shipper_id = ns.shipper_id and driver_hub.report_date = case when status in (11) and ns.drop_real_time > 0 then date(from_unixtime(ns.drop_real_time - 3600))
                                                                                                                   when status in (14) and ns.update_time > 0 then date(from_unixtime(ns.update_time - 3600))
                                                                                                                   else date(from_unixtime(ns.create_time- 3600)) end
        WHERE 1=1
        and date(from_unixtime(ns.create_time - 3600)) >= DATE'2021-08-01' 
        and date(from_unixtime(ns.create_time - 3600)) < date(current_date)

        and ns.city_id <> 238
      --  and ns.status in (6,9,12)
        )base

LEFT JOIN
        (SELECT dot.*, dotet.order_data, group_info.create_time as order_create_time, group_info.group_create_time

         FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
         left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id

         LEFT JOIN
                (
                SELECT ogm.*, ogi.create_time as group_create_time
                FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm

                LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
                WHERE 1=1
                )group_info on group_info.order_id = dot.id and group_info.mapping_status = 11 and group_info.group_id = dot.group_id
          WHERE dot.grass_schema = 'foody_partner_db'
        ) dot on dot.ref_order_id = base.id and dot.ref_order_code = base.order_code

Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = dot.pick_district_id

--- city, district of user
left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live drop_city on drop_city.id = dot.drop_city_id and drop_city.country_id = 86

Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live drop_district on drop_district.id = dot.drop_district_id
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
                ,date(from_unixtime(a.create_time - 3600)) as date_
                ,case when cast(FROM_UNIXTIME(a.create_time - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                      when cast(FROM_UNIXTIME(a.create_time - 3600) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                        else YEAR(cast(FROM_UNIXTIME(a.create_time - 3600) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 3600) as date)) end as year_week

        from (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge

                UNION

                SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge
            )a

            -- take last incharge
            LEFT JOIN
                    (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

                    from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                    where status in (3,4) -- shipper incharge

                    UNION

                    SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

                    from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                    where status in (3,4) -- shipper incharge
                )a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time

            -- auto accept

           LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end

        where 1=1
        and a_filter.order_id is null -- take last incharge
        -- and a.order_id = 9490679
        and a.order_type = 200

        GROUP BY 1,2,3,4,5,6,7,8

        )group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category


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
        )assign on assign.order_id = case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and dot.order_create_time = dot.group_create_time then dot.group_id else base.id end
               and assign.order_type = case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and dot.order_create_time = dot.group_create_time then coalesce(group_order.order_type,0) else base.order_type end
               and assign.order_category = base.order_type and base.created_timestamp <= assign.first_auto_assign_timestamp


LEFT JOIN
        (SELECT ogm.group_id
               ,ogi.group_code
               ,count (distinct dot.ref_order_id) as total_order_in_group
               ,count (distinct case when ogm.create_time = ogi.create_time then dot.ref_order_id else null end ) total_order_in_group_original
         FROM
            (
            SELECT
                   dot.ref_order_id
                  ,dot.ref_order_code
                  ,dot.ref_order_category
                  ,dot.group_id

            FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot) dot

            LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status in (11,26) and ogm.ref_order_category = dot.ref_order_category

            LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status in (11,26)
                                                                                                                                and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                    and ogm_filter.create_time >  ogm.create_time
             LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id
          WHERE 1=1
          and ogm.group_id is not null
          and ogm_filter.create_time is null

          GROUP BY 1,2
        )order_rank on order_rank.group_id = dot.group_id and  order_rank.group_id > 0
--- auto-accept
LEFT JOIN
        (SELECT a.order_type, a.order_id
                , IF(a.experiment_group IN (3,4),1,0) AS is_auto_accepted
                , IF(a.experiment_group IN (7,8),1,0) AS is_auto_accepted_continuous_assign

        FROM
                (SELECT order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge
                and experiment_group in (3,4,7,8) -- auto accept

                UNION ALL

                SELECT order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge
                and experiment_group in (3,4,7,8) -- auto accept
                ) a
        LEFT JOIN
                (SELECT order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge
                and experiment_group in (3,4,7,8) -- auto accept

                UNION ALL

                SELECT order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge
                and experiment_group in (3,4,7,8) -- auto accept
                ) a_filter on a.order_id = a_filter.order_id and a.order_type = a_filter.order_type and a.create_time < a_filter.create_time

        WHERE a_filter.order_id is null -- take last incharge
        GROUP BY 1,2,3,4
        ) auto on base.id = auto.order_id and base.order_type = auto.order_type

WHERE 1=1
and base.order_status <> 'Others'
and base.created_date BETWEEN DATE'2021-09-01' AND DATE'2021-10-31'
and base.city_name IN ('Binh Duong','Can Tho City','Dong Nai','Vung Tau','Hue City','Hai Phong City','Da Nang City')
) base1
GROUP BY 1,2,3,4,5,6
)

UNION ALL
-- NowFood
(SELECT
    inflow_date AS report_date
    , inflow_hour AS report_hour
    , city_name
    , if(is_del = 1, shipper_id, null) as shipper_id
    , if(is_auto_accepted_continuous_assign = 1, 1, 0) as is_CA
    , foody_service
    , sum(cnt_total_order) as gross_orders
    , sum(case when is_del = 1 then cnt_total_order END) as net_orders
    , sum(case when is_canceled = 1 then cnt_total_order END) as canceled_orders
    , sum(case when is_canceled = 1 and cancel_reason = 'No driver' then cnt_total_order END) as cancel_no_driver_orders
FROM
(
SELECT   base2.created_hour_range
        ,base2.created_hour
        ,base2.created_date
        ,base2.created_year_week
        ,base2.created_year_month
        ,base2.cancel_hour
        ,base2.cancel_date
        ,base2.inflow_hour
        ,base2.inflow_date
        ,base2.event_name
        ,base2.city_group
        ,base2.city_name
        ,base2.district_name
        ,base2.distance_range
        ,base2.is_asap
        ,base2.payment_method
        ,base2.foody_service
        ,base2.cancel_reason
        ,case when base2.cancel_reason is null then null
              when base2.cancel_reason in ('No driver') then '4. Driver'
              when base2.cancel_reason in ('Out of stock', 'Shop closed','Shop busy','Shop did not confirm','Wrong price') then '3. Merchant'
              when base2.cancel_reason in ('Pending status from bank') then '5. System'
              when base2.cancel_reason in ('Payment failed') then '2. Buyer System'
              when base2.cancel_reason in ('Affected by quarantine area','Order limit due to Covid') then '6. Others'
              else '1. Buyer Voluntary' end as cancel_by
        ,case when base2.cancel_reason is null then null
              when base2.cancel_reason in ('No driver') then '2. Buyer Non-voluntary Cancellation'
              when base2.cancel_reason in ('Out of stock', 'Shop closed','Shop busy','Shop did not confirm','Wrong price') then '2. Buyer Non-voluntary Cancellation'
              when base2.cancel_reason in ('Pending status from bank') then '2. Buyer Non-voluntary Cancellation'
              when base2.cancel_reason in ('Payment failed') then '2. Buyer Non-voluntary Cancellation'
              when base2.cancel_reason in ('Affected by quarantine area','Order limit due to Covid') then '2. Buyer Non-voluntary Cancellation'
              else '1. Buyer Voluntary Cancellation' end as cancel_type
        ,base2.is_canceled
        ,base2.is_del
        ,base2.shipper_id
        ,base2.is_auto_accepted_continuous_assign
        ,count(distinct base2.uid) cnt_total_order
        ,count(distinct case when base2.shipper_id > 0 and base2.is_del = 1 then base2.uid else null end) cnt_total_order_for_late_calculation

FROM
(
SELECT   base1.id
        ,base1.uid
        ,base1.cancel_hour
        ,base1.cancel_date
        ,base1.inflow_hour
        ,base1.inflow_date
        ,base1.created_hour
        ,base1.created_hour_range
        ,base1.created_date
        ,base1.created_year_week
        ,base1.created_year_month
      --  ,base1.merchant_id
    --    ,base1.merchant_name
        ,base1.shipper_id
        ,base1.event_name
        ,base1.city_group
        ,base1.city_name
        ,base1.is_asap
        ,base1.district_name
        ,base1.order_status
        ,base1.payment_method
        ,base1.foody_service
        ,case when trim(base1.cancel_reason) = 'Shop closed' then (case when po.is_pre_order> 0 then 'Pre-order' else 'Shop closed' end)
              else base1.cancel_reason end as cancel_reason

        ,case when order_status = 'Cancelled' then 1 else 0 end as is_canceled
        ,case when order_status = 'Delivered' then 1 else 0 end as is_del
        ,case when order_status = 'Quit' then 1 else 0 end as is_quit
        ,base1.distance_range
        ,base1.is_auto_accepted
        ,base1.is_auto_accepted_continuous_assign
FROM
(
SELECT
         base.id
        ,base.uid
        ,base.cancel_hour
        ,base.cancel_date
        ,base.created_hour
        ,base.created_hour_range
        ,base.created_date
        ,base.created_year_week
        ,base.created_year_month
        ,case when base.created_date = date('2020-11-11') then '1. Camp.11.11 (2020)'
              when base.created_date = date('2020-11-20') then '2. Camp.20.11 (2020)'
              when base.created_date = date('2020-12-12') then '3. Camp.12.12 (2020)'
              when base.created_date = date('2021-01-11') then '4. Camp.11.01 (2021)'
              when base.created_date = date('2021-01-22') then '5. Camp.22.01 (2021)'
              when base.created_date = date('2021-01-27') then '6. Camp.27.01 (2021)'
              when base.created_date = date('2021-03-03') then '7. Camp.03.03 (2021)'
              when base.created_date = date('2021-03-08') then '8. Camp.08.03 (2021)'
              when base.created_date = date('2021-03-27') then '9. Camp.27.03 (2021)'
              when base.created_date = date('2021-04-04') then '10. Camp.04.04 (2021)'
              when base.created_date = date('2021-04-15') then '11. Camp.15.04 (2021)'
              when base.created_date = date('2021-05-05') then '12. Camp.05.05 (2021)'
              when base.created_date = date('2021-06-06') then '13. Camp.06.06 (2021)'
              when base.created_date = date('2021-07-07') then '14. Camp.07.07 (2021)'
              when base.created_date = date('2021-07-24') then '15. Camp.07.24 (2021)'
              when base.created_date = date('2021-08-08') then '16. Camp.08.08 (2021)'
              when base.created_date = date('2021-09-09') then '17. Camp.09.09 (2021)'
              when base.created_date = date('2021-10-10') then '18. Camp.10.10 (2021)'
              when base.created_date = date('2021-10-20') then '19. Camp.10.20 (2021)'
              when base.created_date = date('2021-11-11') then '20. Camp.11.11 (2021)'
              when base.created_date = date('2021-11-20') then '21. Camp.11.20 (2021)'
              when base.created_date = date('2021-12-12') then '22 Camp.12.12 (2021)'
              else null end as event_name
        ,extract(hour from inflow_timestamp) inflow_hour
        ,date(inflow_timestamp) inflow_date
        ,base.shipper_id
        ,base.city_group
        ,base.city_name
        ,base.district_name
        ,base.distance_range
        ,base.is_asap
        ,base.order_status
        ,base.payment_method
        ,base.foody_service
        ,base.cancel_reason
        ,base.cancel_type_id
        ,auto.is_auto_accepted
        ,auto.is_auto_accepted_continuous_assign
FROM
        (-- order delivery: Food/Market
        SELECT   oct.id
                ,concat('order_delivery_',cast(oct.id as VARCHAR)) as uid
                ,oct.shipper_uid as shipper_id
                ,from_unixtime(oct.submit_time - 3600) as created_timestamp
                ,date(from_unixtime(oct.submit_time - 3600)) as created_date
                ,oct.submit_time
                ,case when cast(from_unixtime(oct.submit_time - 3600) as date) between DATE('2018-12-31') and DATE('2018-12-31') then 201901
                    when cast(from_unixtime(oct.submit_time - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                    when cast(from_unixtime(oct.submit_time - 3600) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                    else YEAR(cast(from_unixtime(oct.submit_time - 3600) as date))*100 + WEEK(cast(from_unixtime(oct.submit_time - 3600) as date)) end as created_year_week
                ,concat(cast(YEAR(from_unixtime(oct.submit_time - 3600)) as VARCHAR),'-',cast(date_format(from_unixtime(oct.submit_time - 3600),'%m') as VARCHAR)) as created_year_month
                ,case when oct.status = 7 then 'Delivered'
                    when oct.status = 8 then 'Cancelled'
                    when oct.status = 9 then 'Quit' end as order_status
                ,city.name_en as city_name
                ,oct.city_id
                ,case when oct.city_id = 217 then 'HCM'
                    when oct.city_id = 218 then 'HN'
                    when oct.city_id = 219 then 'DN'
                    ELSE 'OTH' end as city_group
                ,oct.distance
                ,case
                    when oct.distance <= 3 then '1. 0-3km'
                    when oct.distance <= 4 then '2. 3-4km'
                    when oct.distance <= 5 then '3. 4-5km'
                    when oct.distance <= 7 then '4. 5-7km'
                    when oct.distance > 7 then '5. 7km+'
                    else null end as distance_range

                ,case when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 5 then '5. 22:00-6:00'
                    when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 10 then '1. 6:00-11:00'
                    when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 13 then '2. 11:00-14:00'
                    when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 17 then '3. 14:00-18:00'
                    when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 21 then '4. 18:00-22:00'
                    when Extract(HOUR from from_unixtime(oct.submit_time - 3600)) <= 23 then '5. 22:00-6:00'
                    else null end as created_hour_range
                ,Extract(HOUR from from_unixtime(oct.submit_time - 3600)) created_hour
                ,oct.foody_service_id
                ,case when oct.foody_service_id = 1 then 'Food'
                        when oct.foody_service_id in (5) then 'Fresh'
                        else 'Market' end as foody_service

                ,case when oct.payment_method = 1 then 'Cash'
                        when oct.payment_method = 6 then 'AP'
                        when oct.payment_method = 4 then 'Card'
                        when oct.payment_method = 8 then 'VNPay/ibanking'
                        when oct.payment_method = 12 then 'AP credit card'
                        when oct.payment_method = 3 then 'Bank transfer'
                        when oct.payment_method = 7 then 'Momo'
                        else 'Others' end as payment_method
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
                        else 'Others' end as cancel_reason

                ,oct.is_asap
                -- incharge time
                ,osl.first_auto_assign_timestamp
                ,osl.last_incharge_timestamp
                ,osl.last_cancel_timestamp
                ,extract(hour from osl.last_cancel_timestamp) cancel_hour
                ,date(osl.last_cancel_timestamp) cancel_date
                ,coalesce(osl.first_auto_assign_timestamp, from_unixtime(oct.submit_time - 3600)) inflow_timestamp
                ,date_diff('second',osl.first_auto_assign_timestamp,osl.last_incharge_timestamp) as lt_incharge -- from 1st auto assign to last incharge
                ,from_unixtime(oct.final_delivered_time - 3600) last_delivered_timestamp
                ,from_unixtime(oct.estimated_delivered_time - 3600) estimated_delivered_timestamp

                ,from_unixtime(go.confirm_timestamp - 3600) confirm_timestamp
                ,from_unixtime(go.pick_timestamp - 3600) pick_timestamp
                ,date_diff('second',from_unixtime(go.confirm_timestamp - 3600),from_unixtime(go.pick_timestamp - 3600)) lt_merchant_prep

                ,district.name_en as district_name


                ,case when oct.merchant_paid_method = 6 and oct.status in (7) then 1 else 0 end as is_nmw
                ,go.is_now_merchant_order_flag


                ,go.cancel_type_id
        from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
        -- left join shopeefood.foody_mart__profile_shipper_master shp on shp.shipper_id = oct.shipper_uid
        left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86

        left join shopeefood.foody_mart__fact_gross_order_join_detail go on go.id = oct.id
        Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = oct.district_id
        left join shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr on cr.id = try_cast(json_extract_scalar(oct.extra_data,'$.note_ids') as int) -- note_ids: cancel_reason
        Left join
                (SELECT oct.id as order_id, oct.uid as shipper_id
                      ,oct.shipping_info_id
                      ,cast(json_extract(si.extra_data,'$.location_info.district_id') as bigint) drop_district_id
                      ,drop_district.name_en as drop_district_name
                      ,cast(json_extract(si.extra_data,'$.location_info.province_id') as bigint) drop_city_id
                      ,city.name_en as drop_city_name
                FROM shopeefood.foody_order_db__order_completed_search_tab__reg_continuous_s0_live oct
                LEFT JOIN shopeefood.foody_delivery_db__shipping_info_tab__reg_daily_s0_live si on oct.shipping_info_id = si.shipping_id
                Left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live drop_district on drop_district.id = cast(json_extract(si.extra_data,'$.location_info.district_id') as bigint)

                left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = cast(json_extract(si.extra_data,'$.location_info.province_id') as bigint) and city.country_id = 86

                WHERE 1=1

                group by 1,2,3,4,5,6,7
                )drop_district on drop_district.order_id = oct.id

        -- assign time: request archive log
        left join
            (SELECT order_id
                ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,max(case when status = 7 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_delivered_timestamp
                ,min(case when status = 13 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_confirmed_timestamp
                ,max(case when status = 9 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_cancel_timestamp
            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
            where 1=1
            group by order_id
            )osl on osl.order_id = oct.id


        WHERE 1=1
        and date(from_unixtime(oct.submit_time - 3600)) >= DATE'2021-08-01' 
        and date(from_unixtime(oct.submit_time - 3600)) < date(current_date)
      --  and oct.foody_service_id = 1
        and oct.city_id <> 238

        )base

--- auto-accept
LEFT JOIN
        (SELECT a.order_type, a.order_id
                , IF(a.experiment_group IN (3,4),1,0) AS is_auto_accepted
                , IF(a.experiment_group IN (7,8),1,0) AS is_auto_accepted_continuous_assign

        FROM
                (SELECT order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge
                and experiment_group in (3,4,7,8) -- auto accept

                UNION ALL

                SELECT order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge
                and experiment_group in (3,4,7,8) -- auto accept
                ) a
        LEFT JOIN
                (SELECT order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge
                and experiment_group in (3,4,7,8) -- auto accept

                UNION ALL

                SELECT order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                where status in (3,4) -- shipper incharge
                and experiment_group in (3,4,7,8) -- auto accept
                ) a_filter on a.order_id = a_filter.order_id and a.order_type = a_filter.order_type and a.create_time < a_filter.create_time

        WHERE a_filter.order_id is null -- take last incharge
        GROUP BY 1,2,3,4
        ) auto on base.id = auto.order_id and auto.order_type = 0

)base1

LEFT JOIN

            (SELECT
                    base.id
                    ,sum(base.is_pre_order) as is_pre_order


            FROM

                    (select oct.id
                            ,date(from_unixtime(oct.submit_time - 3600)) as submit_date
                            ,case when trim(split(cast(json_extract(bo.note_content, '$.default') as VARCHAR),':',2)[2]) = 'Wrong Pre-order' then 1 else 0 end is_pre_order

                    from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct

                    left join shopeefood.foody_mart__profile_merchant_master mpm on oct.restaurant_id = mpm.merchant_id and mpm.grass_date = 'current' -- merchant name: take current
                    left join shopeefood.foody_mart__fact_order_note bo on bo.order_id = oct.id and bo.note_type_id = 2 -- note_type_id = 2 --> bo reason
                                                         and COALESCE(cast(json_extract(bo.note_content, '$.default') as VARCHAR),cast(json_extract(bo.note_content, '$.en') as VARCHAR), bo.extra_note) != ''

                    where 1=1
                    and date(from_unixtime(oct.submit_time - 3600)) >= DATE'2021-08-01' 
                    and oct.foody_service_id = 1


                    ) base

            group by 1

            ) po
ON base1.id = po.id
)base2

WHERE 1=1
and base2.created_date >= date'2020-10-01'
-- and city_name not in ('Phu Yen','Binh Dinh','Thanh Hoa','Dak Lak','Gia Lai','Ha Tinh')
and base2.city_name IN ('Binh Duong','Can Tho City','Dong Nai','Vung Tau','Hue City','Hai Phong City','Da Nang City')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
)
WHERE inflow_date BETWEEN DATE'2021-09-01' AND DATE'2021-10-31'
GROUP BY 1,2,3,4,5,6
)
)
GROUP BY 1,2,3,4,5
ORDER BY 3,4,1,2