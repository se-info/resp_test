--transacting
WITH tier_tab AS 
(SELECT 
         CAST(from_unixtime(bonus.report_date - 60*60) AS DATE) as report_date
        ,bonus.uid as shipper_id
        ,CASE 
              WHEN sm.shipper_type_id = 12 THEN 'Hub' ELSE ti.tier_name_en END AS shipper_tier
        ,sm.city_name              
        ,bonus.total_point
        ,bonus.daily_point
        ,completed_rate/CAST(100 AS DOUBLE) AS sla_rate

FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm 
    on sm.shipper_id = bonus.uid 
    and try_cast(sm.grass_date as date) = date(from_unixtime(bonus.report_date - 3600))

LEFT JOIN shopeefood.foody_internal_db__shipper_tier_config_tab__reg_daily_s0_live ti 
    on ti.tier_id = bonus.tier 
    and ti.city_id = sm.city_id
)
,order_tab AS
(SELECT 
        DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) AS report_date
       ,YEAR(DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)))*100 + MONTH(DATE(FROM_UNIXTIME(dot.real_drop_time - 3600))) AS year_month   
       ,dot.uid AS shipper_id 
       ,tt.shipper_tier
       ,tt.city_name
       ,COUNT(DISTINCT order_code) AS total_order 
       ,COUNT(DISTINCT CASE WHEN ref_order_category != 0 THEN order_code ELSE NULL END) AS total_order_spxi


FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day ) dot 

LEFT JOIN tier_tab tt 
    on tt.shipper_id = dot.uid 
    and tt.report_date = DATE(FROM_UNIXTIME(dot.real_drop_time - 3600))

WHERE 1 = 1 
AND dot.pick_city_id IN (217,218)
AND DATE(FROM_UNIXTIME(dot.real_drop_time - 3600)) BETWEEN date'2022-07-01' AND current_date - interval '1' day
AND dot.order_status IN (400,405)
GROUP BY 1,2,3,4,5
)

SELECT 
         year_month 
        ,city_name 
        ,shipper_tier
        ,COUNT(shipper_id)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) AS a1 
        ,COUNT(CASE WHEN total_order_spxi > 0 THEN shipper_id ELSE NULL END)/CAST(COUNT(DISTINCT report_date) AS DOUBLE) a1_spxi
        ,COUNT(DISTINCT shipper_id) AS a30 
        ,COUNT(DISTINCT CASE WHEN total_order_spxi > 0 THEN shipper_id ELSE NULL END) AS a30_spxi 



FROM order_tab


WHERE city_name IN ('HCM City','Ha Noi City')
GROUP BY 1,2,3

-- order performance
WITH assignment AS 
(SELECT 
        sa.order_id
       ,COALESCE(ogm.ref_order_id,dot.ref_order_id) AS ref_order_id 
       ,COALESCE(ogm.ref_order_code,dot.ref_order_code) AS order_code
       ,COALESCE(ogi.ref_order_category,sa.order_type) AS order_category
       ,sa.status
       ,sa.shipper_uid AS driver_id
       ,FROM_UNIXTIME(sa.create_time - 3600) AS create_time
       ,FROM_UNIXTIME(sa.update_time - 3600) AS update_time
       ,CASE 
            WHEN sa.assign_type = 1 then '1. Single Assign'
            WHEN sa.assign_type in (2,4) then '2. Multi Assign'
            WHEN sa.assign_type = 3 then '3. Well-Stack Assign'
            WHEN sa.assign_type = 5 then '4. Free Pick'
            WHEN sa.assign_type = 6 then '5. Manual'
            WHEN sa.assign_type in (7,8) then '6. New Stack Assign'
            ELSE NULL END AS assign_type
       ,CASE 
            WHEN sa.order_type = 200 then 'Group'
            ELSE 'Single' END AS order_type
               

FROM 
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live


        UNION
    
        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
) sa 

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogi
    on ogi.id = (CASE WHEN sa.order_type = 200 THEN sa.order_id ELSE 0 END)

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) ogm 
    on ogm.group_id = ogi.id
    and ogm.ref_order_category = ogi.ref_order_category
    and ogm.create_time <= sa.create_time

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da WHERE DATE(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = (CASE WHEN sa.order_type = 200 THEN ogm.ref_order_id ELSE sa.order_id END) 
    and dot.ref_order_category = (CASE WHEN sa.order_type = 200 THEN ogm.ref_order_category ELSE sa.order_type END)

WHERE 1 = 1
AND DATE(FROM_UNIXTIME(sa.create_time - 3600)) BETWEEN DATE'2022-07-01' AND current_date - interval '1' day
)
,order_tab AS 
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
        ,sa.first_incharge_timestamp
        ,sa.last_incharge_timestamp
        ,COALESCE(sa.no_assign,0) AS no_assign
        ,COALESCE(sa.no_incharged,0) AS no_incharged
        ,COALESCE(sa.no_deny,0) AS no_deny
        ,COALESCE(sa.no_ignored,0) AS no_ignored
        ,from_unixtime(ns.pick_real_time - 3600) picked_timestamp
        ,0 AS bad_weather_fee
        ,0 AS late_night_service_fee
        ,0 AS holiday_service_fee


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


from shopeefood.foody_express_db__booking_tab__reg_daily_s0_live

UNION

SELECT  
         id
        ,concat('now_ship_shopee_',cast(id as VARCHAR)) as uid
        ,code
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
(SELECT                      ns.ref_order_id, ns.order_category
                            ,count(ns.order_id) no_assign
                            ,count(case when status in (3,4) then order_id else null end) no_incharged
                            ,count(case when status in (8,9,17,18) then order_id else null end) no_ignored
                            ,count(case when status in (2,14,15) then order_id else null end) no_deny
                            ,min(create_time) first_auto_assign_timestamp
                            ,min(case when status in (3,4) then update_time else null end) as first_incharge_timestamp
                            ,max(case when status in (3,4) then update_time else null end) as last_incharge_timestamp
                            ,max(case when status in (3,4) then update_time else null end) as last_picked_timestamp 
                    FROM assignment ns 
                    where order_category in (4,5,6,7)                             
                    GROUP BY 1,2
) sa on sa.ref_order_id = ns.id 
    and (case when ns.booking_type = 2 and ns.booking_service_type = 1 then 4
                when ns.booking_type = 3 and ns.booking_service_type = 1 then 5
                when ns.booking_type = 4 and ns.booking_service_type = 1 then 6
                when ns.booking_type = 2 and ns.booking_service_type = 2 then 7
                when ns.booking_type = 2 and ns.booking_service_type = 3 then 8
                else null end) = sa.order_category 

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

WHERE DATE(from_unixtime(ns.create_time - 3600)) BETWEEN DATE'2022-07-01' AND current_date - interval '1' day
)
SELECT 
        YEAR(created_date)*100 + MONTH(created_date) AS year_month 
       ,city_group
       ,COUNT(DISTINCT uid)/CAST(COUNT(DISTINCT created_date) AS DOUBLE) AS gross_order 
       ,COUNT(DISTINCT CASE WHEN order_status IN ('Delivered','Returned') THEN uid ELSE NULL END)/CAST(COUNT(DISTINCT created_date) AS DOUBLE) AS net_order 
       ,COUNT(DISTINCT CASE WHEN order_status IN ('Delivered','Returned') THEN uid ELSE NULL END)/CAST(COUNT(DISTINCT uid) AS DOUBLE) AS g2n
       ,COUNT(DISTINCT CASE WHEN order_status = 'Assigning Timeout' THEN uid ELSE NULL END)/CAST(COUNT(DISTINCT uid) AS DOUBLE) AS timeout_order
       ,COUNT(DISTINCT CASE WHEN order_status = 'Assigning Timeout' AND no_assign = 0 THEN uid ELSE NULL END)/CAST(COUNT(DISTINCT uid) AS DOUBLE) AS timeout_no_assign_order
       ,COUNT(DISTINCT CASE WHEN order_status = 'Pickup Failed' THEN uid ELSE NULL END)/CAST(COUNT(DISTINCT uid) AS DOUBLE) AS pickup_failed_order
       ,SUM(no_incharged+no_deny)/CAST(SUM(no_assign) AS DOUBLE) AS accept 
       ,SUM(no_deny)/CAST(SUM(no_assign) AS DOUBLE) AS denied 
       ,SUM(no_ignored)/CAST(SUM(no_assign) AS DOUBLE) AS ignored





FROM order_tab

WHERE city_group IN ('HCM','HN')
GROUP BY 1,2