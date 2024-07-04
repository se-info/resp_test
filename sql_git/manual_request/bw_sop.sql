
SELECT
     inflow_date
    ,city_name
    ,if(foody_service = 'Food', 'Food', 'Fresh/Market') as service
    ,distance_range
    ,is_bw
    ,sum(if(cancel_reason = 'No driver' and is_canceled = 1, cnt_total_order, 0))/cast(count(distinct inflow_date) as double) as cnt_cancel_no_driver
    ,sum(cnt_total_order)/cast(count(distinct inflow_date) as double) as total_submit
    ,sum(total_net)/cast(count(distinct inflow_date) as double) as total_net
    ,sum(case when is_asap = 1 then sum_incharged_time else null end)/cast(sum(case when is_asap = 1 then total_net else null end) as double) as avg_incharged_time
    ,sum(case when is_asap = 1 then sum_pickup_time else null end)/cast(sum(case when is_asap = 1 then total_net else null end) as double) as avg_pickup_time
    ,sum(case when is_asap = 1 then sum_pickup_delivered else null end)/cast(sum(case when is_asap = 1 then total_net else null end) as double) as avg_pickup_delivered_time
    -- ,sum(total_a1)
FROM
    (
    SELECT *

FROM
(
SELECT   base2.created_date
        ,base2.cancel_date
        ,base2.inflow_date
        ,base2.inflow_hour
        ,base2.city_group
        ,base2.city_name
        ,base2.is_asap
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
        ,base2.is_bw
        ,base2.distance_range
        ,count(distinct base2.uid) cnt_total_order
        ,count(distinct case when base2.order_status = 'Delivered' then base2.uid else null end) as total_net
        ,count(distinct case when base2.shipper_id > 0 and base2.is_del = 1 then base2.uid else null end) cnt_total_order_for_late_calculation
        ,sum(base2.lt_incharged) as sum_incharged_time
        ,sum(base2.lt_pickup) as sum_pickup_time
        ,sum(base2.lt_pickup_delivered) as sum_pickup_delivered
        ,count(distinct case when base2.order_status = 'Delivered' then base2.shipper_id else null end) as total_a1

FROM
(
SELECT   base1.id
        ,base1.uid
        ,base1.cancel_date
        ,base1.inflow_date
        ,base1.created_date
        ,base1.inflow_hour
        ,base1.shipper_id
        ,base1.city_group
        ,base1.city_name
        ,base1.is_asap
        ,base1.order_status
        ,base1.foody_service
        ,case when trim(base1.cancel_reason) = 'Shop closed' then (case when po.is_pre_order> 0 then 'Pre-order' else 'Shop closed' end)
              else base1.cancel_reason end as cancel_reason

        ,case when order_status = 'Cancelled' then 1 else 0 end as is_canceled
        ,case when order_status = 'Delivered' then 1 else 0 end as is_del
        ,case when order_status = 'Quit' then 1 else 0 end as is_quit
        ,base1.is_bw
        ,base1.distance_range
        ,base1.lt_incharged
        ,base1.lt_pickup
        ,base1.lt_pickup_delivered
FROM
(
SELECT
         base.id
        ,base.uid
        ,base.cancel_date
        ,base.created_date
        ,date(inflow_timestamp) inflow_date
        ,extract(hour from inflow_timestamp) as inflow_hour
        ,base.shipper_id
        ,base.city_group
        ,base.city_name
        ,base.is_asap
        ,base.order_status
        ,base.foody_service
        ,base.cancel_reason
        ,base.distance_range
        ,case when base.bad_weather_fee > 0 then 1 else 0 end as is_bw
        ,base.bad_weather_fee
        ,base.lt_incharged
        ,base.lt_pickup
        ,base.lt_pickup_delivered
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
                ,city.name_en AS city_name
                ,oct.city_id
                ,case when oct.city_id = 217 then 'HCM'
                    when oct.city_id = 218 then 'HN'
                    when oct.city_id = 219 then 'DN'
                    ELSE 'OTH' end as city_group
                ,oct.foody_service_id
                ,oct.distance
                ,case when oct.distance < 3 then '1. < 3km'
                      when oct.distance < 5 then '2. 3 - 5km'                                          
                      when oct.distance < 7 then '3. 5 - 7km'
                      when oct.distance <= 10 then '4. 7 - 10km'
                      when oct.distance > 10 then '5. > 10km' 
                      end as distance_range 
                ,go.bad_weather_fee
                ,case when oct.foody_service_id = 1 then 'Food'
                        when oct.foody_service_id in (5) then 'Market - Fresh'
                        else 'Market - Non Fresh' end as foody_service
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
                ,date(osl.last_cancel_timestamp) cancel_date
                ,coalesce(osl.first_auto_assign_timestamp, from_unixtime(oct.submit_time - 3600)) inflow_timestamp
                ,date_diff('second',from_unixtime(oct.submit_time - 3600),osl.last_incharge_timestamp)/cast(60 as double) as lt_incharged
                ,date_diff('second',osl.last_incharge_timestamp,osl.last_picked_timestamp)/cast(60 as double) as lt_pickup
                ,date_diff('second',osl.last_picked_timestamp,osl.last_delivered_timestamp)/cast(60 as double) as lt_pickup_delivered

            ,case 
                when oct.cancel_type in (0,5) then 'System'
                when oct.cancel_type = 1 then 'CS BPO'
                when oct.cancel_type = 2 then 'User'
                when oct.cancel_type in (3,4) then 'Merchant'
                when oct.cancel_type = 6 then 'Fraud'
                end as cancel_actor

        from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct
        left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86

        left join 
                (select * from shopeefood.foody_mart__fact_gross_order_join_detail 
                          where  date(grass_date) >= current_date - interval '45' day    )go on go.id = oct.id
        left join shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr on cr.id = try_cast(json_extract_scalar(oct.extra_data,'$.note_ids') as int) -- note_ids: cancel_reason
        -- assign time: request archive log
        left join
            (SELECT order_id
                ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,max(case when status = 7 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_delivered_timestamp
                ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
                ,min(case when status = 13 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_confirmed_timestamp
                ,max(case when status = 9 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_cancel_timestamp
            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
            where 1=1
            group by order_id
            )osl on osl.order_id = oct.id
        WHERE 1=1

        and date(from_unixtime(oct.submit_time - 3600)) >= current_date - interval '45' day 
      --  and oct.foody_service_id = 1
        and oct.city_id <> 238
        )base
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
                    left join
                             (SELECT *

                              FROM shopeefood.foody_mart__fact_order_note

                              WHERE 1=1
                              and grass_region ='VN'
                              )bo on bo.order_id = oct.id and bo.note_type_id = 2 -- note_type_id = 2 --> bo reason
                                                         and COALESCE(cast(json_extract(bo.note_content, '$.default') as VARCHAR),cast(json_extract(bo.note_content, '$.en') as VARCHAR), bo.extra_note) != ''

                    where 1=1

                    --and oct.foody_service_id = 1
                    ) base
            group by 1
            ) po
ON base1.id = po.id
)base2

WHERE 1=1

and city_name not in ('Phu Yen','Binh Dinh','Thanh Hoa','Dak Lak','Gia Lai','Ha Tinh')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)base3
    )

where 1 = 1 

and inflow_date between date'2022-08-06' and date'2022-08-12'

and city_name in ('HCM City','Ha Noi City')

GROUP BY 1,2,3,4,5



