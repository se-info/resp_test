with raw_date as 
(SELECT

      DATE(report_date) AS report_date
FROM
    (
(
SELECT sequence(date'2023-02-13',date'2023-03-05') bar)

CROSS JOIN
    unnest (bar) as t(report_date)
)
)
,params_date(period_group,period,start_date,end_date,days) as 
(
SELECT 
        '1. Daily'
        ,CAST(report_date as varchar)
        ,CAST(report_date as date)
        ,CAST(report_date as date)
        ,CAST(1 as double)

from raw_date

UNION 

SELECT 
        '2. Weekly'
        ,'W- ' || CAST(YEAR(date_trunc('week',report_date))*100 + WEEK(date_trunc('week',report_date)) as varchar)
        ,CAST(date_trunc('week',report_date) as date)
        ,max(report_date)
        ,date_diff('day',cast(date_trunc('week',report_date) as date),max(report_date)) + 1

from raw_date

group by 1,2,3
)
,assignment as 
(SELECT 
        sa.order_id
       ,COALESCE(ogm.ref_order_id,dot.ref_order_id) AS ref_order_id 
       ,COALESCE(ogm.ref_order_code,dot.ref_order_code) AS order_code
       ,COALESCE(ogi.ref_order_category,sa.order_type) AS order_category
       ,sa.status
       ,sa.shipper_uid AS driver_id
       ,FROM_UNIXTIME(sa.create_time - 3600) AS create_time
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
       ,dot.order_status
       ,cast(json_extract(doet.order_data,'$.shipper_policy.type') as bigint) AS driver_payment_policy                           

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

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) doet 
    on doet.order_id = dot.id


WHERE 1 = 1
AND DATE(FROM_UNIXTIME(sa.create_time - 3600)) BETWEEN current_date - interval '30' day AND current_date - interval '1' day
)
,raw as 
(SELECT          oct.id
                ,concat('order_delivery_',cast(oct.id as VARCHAR)) as order_uid
                ,oct.shipper_uid as shipper_id
                ,from_unixtime(oct.submit_time - 3600) as created_timestamp
                ,HOUR(from_unixtime(oct.submit_time - 3600)) as created_hour
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
                ,oct.restaurant_id
                ,dt.name_en as district_name
                ,case when oct.distance < 3 then '1. < 3km'
                      when oct.distance < 5 then '2. 3 - 5km'  
                      when oct.distance < 7 then '3. 5 - 7km'
                      when oct.distance <= 10 then '4. 7 - 10km'
                      when oct.distance > 10 then '5. > 10km' 
                      end as distance_range 
                ,case when oct.foody_service_id = 1 then 'Food'
                        when oct.foody_service_id in (5) then 'Market - Fresh'
                        else 'Market - Non Fresh' end as foody_service
                ,cast(json_extract(oct.extra_data,'$.total_item') as bigint) as item        
                ,case when cast(json_extract(oct.extra_data,'$.total_item') as bigint) <= 10 then '1. 0 - 10 items'
                    when cast(json_extract(oct.extra_data,'$.total_item') as bigint) <= 20 then '2. 10 - 20 items'
                    when cast(json_extract(oct.extra_data,'$.total_item') as bigint) <= 30 then '3. 20 - 30 items'
                    when cast(json_extract(oct.extra_data,'$.total_item') as bigint) <= 40 then '4. 30 - 40 items'
                    when cast(json_extract(oct.extra_data,'$.total_item') as bigint) > 40 then '5. > 40 items'
                    end as item_range
                ,case when total_amount/cast(100 as double) <= 100000 then '1. 0 - 100,000 vnd'
                    when total_amount/cast(100 as double) <= 200000 then '2. 100,000 - 200,000 vnd' 
                    when total_amount/cast(100 as double) <= 400000 then '3. 200,000 - 400,000 vnd'
                    when total_amount/cast(100 as double) <= 500000 then '4. 400,000 - 500,000 vnd' 
                    when total_amount/cast(100 as double) > 500000 then '5. > 500,000 vnd'   
                    end as total_amount_range
                ,trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) as reason_2                                
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
                        when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) in ('Merchant cannot accept the order at the moment','Hiện tại Quán không thể tiếp nhận thêm đơn') then 'Order limit due to Covid'
                        when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) = '' then 'Others'
                        when trim(coalesce(cr.message_en, json_extract_scalar(oct.extra_data, '$.cancel_note'))) is null then 'Others'
                        else 'Others' end as cancel_reason

                ,oct.is_asap
                ,case when oct.distance <= 1 then 30
                    when oct.distance > 1 then least(60,30 + 5*(ceiling(oct.distance) -1))
                    else null end as lt_sla
                ,date(osl.last_cancel_timestamp) cancel_date
                ,HOUR(osl.last_cancel_timestamp) cancel_hour
                ,coalesce(date_diff('second',from_unixtime(oct.submit_time-3600),osl.last_cancel_timestamp)/cast(60 as double),0) as lt_submit_cancel
                ,coalesce(osl.first_auto_assign_timestamp, from_unixtime(oct.submit_time - 3600)) inflow_timestamp
                ,coalesce(date_diff('second',from_unixtime(oct.submit_time-3600),from_unixtime(oct.final_delivered_time -3600))/cast(60 as double),0) as lt_submit_delivered
                ,coalesce(date_diff('second',from_unixtime(oct.submit_time - 3600),osl.last_incharge_timestamp)/cast(60 as double),0) as lt_incharged
                ,coalesce(date_diff('second',osl.last_incharge_timestamp,osl.last_picked_timestamp)/cast(60 as double),0) as lt_pickup
                ,coalesce(date_diff('second',osl.last_picked_timestamp,osl.last_delivered_timestamp)/cast(60 as double),0) as lt_pickup_delivered

            ,case 
                when oct.cancel_type in (0,5) then 'System'
                when oct.cancel_type = 1 then 'CS BPO'
                when oct.cancel_type = 2 then 'User'
                when oct.cancel_type in (3,4) then 'Merchant'
                when oct.cancel_type = 6 then 'Fraud'
                end as cancel_actor

        from (select * from shopeefood.shopeefood_mart_dwd_vn_order_completed_da where date(dt) = current_date - interval '1' day ) oct
        left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86
        left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live dt on dt.id = oct.district_id 
        left join shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr on cr.id = try_cast(json_extract_scalar(oct.extra_data,'$.note_ids') as int) -- note_ids: cancel_reason
        -- assign time: request archive log
        left join
            (SELECT order_id
                ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,max(case when status = 7 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_delivered_timestamp
                ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
                ,min(case when status = 13 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_confirmed_timestamp
                ,max(case when status = 8 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_cancel_timestamp
            from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
            where 1=1
            group by order_id
            )osl on osl.order_id = oct.id
        WHERE 1=1

        and date(from_unixtime(oct.submit_time - 3600)) >= current_date - interval '60' day 
        and oct.city_id <> 238

)
,summary as
(SELECT 
        raw.created_date
       ,raw.city_group
       ,COUNT(DISTINCT raw.id) AS gross_order
       ,COUNT(DISTINCT CASE WHEN raw.order_status = 'Delivered' then raw.id ELSE NULL END) AS net_order 
       ,COUNT(DISTINCT CASE WHEN raw.order_status = 'Cancelled' and raw.cancel_reason = 'No driver' then raw.id ELSE NULL END) AS cnd_order
       ,COUNT(CASE WHEN asm.status in (3,4,2,14,15,8,9,17,18) then asm.order_id ELSE NULL END) AS total_assign
       ,COUNT(CASE WHEN asm.status in (8,9,17,18) then asm.order_id ELSE NULL END) as ignore_order
       ,COUNT(CASE WHEN asm.status in (3,4) then asm.order_id ELSE NULL END) as incharge_order
       ,COUNT(CASE WHEN asm.status in (2,14,15) then asm.order_id ELSE NULL END) as deny_order
    --    ,MAX_BY(asm.order_type,asm.assign_time) AS order_type
    --    ,MAX_BY(asm.assign_time,asm.assign_time) AS assign_type

FROM raw 

LEFT JOIN assignment asm on asm.ref_order_id = raw.id

WHERE 1 = 1 

GROUP BY 1,2)

SELECT 
        p.period
       ,p.period_group
       ,s.city_group
       ,SUM(gross_order)/CAST(p.days AS DOUBLE) AS gross_order 
       ,SUM(net_order)/CAST(p.days AS DOUBLE) AS net_order 
       ,SUM(cnd_order)/CAST(p.days AS DOUBLE) AS cnd_order 
       ,SUM(total_assign)/CAST(p.days AS DOUBLE) AS total_assign 
       ,SUM(incharge_order)/CAST(p.days AS DOUBLE) AS incharge_order 
       ,SUM(ignore_order)/CAST(p.days AS DOUBLE) AS ignore_order 
       ,SUM(deny_order)/CAST(p.days AS DOUBLE) AS deny_order 
       ,p.days 



FROM summary s 

INNER JOIN params_date p on s.created_date between p.start_date and p.end_date

WHERE s.city_group not in ('HCM','HN')
GROUP BY 1,2,3,p.days