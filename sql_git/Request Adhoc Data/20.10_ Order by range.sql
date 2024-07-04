with mex_filter as 
(SELECT
        DISTINCT 
        merchant_id
       ,merchant_name
       ,brand_name 

FROM dev_vnfdbi_commercial.shopeefood_vn_food_mex_list_raw_for_daily_bd_meeting_tab

WHERE segment = 'Enterprise')
,order_raw as 
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
                ,oct.restaurant_id as merchant_id
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
                ,date(osl.last_cancel_timestamp) cancel_date
                ,HOUR(osl.last_cancel_timestamp) cancel_hour
                ,coalesce(date_diff('second',from_unixtime(oct.submit_time-3600),osl.last_cancel_timestamp)/cast(60 as double),0) as lt_submit_cancel
                ,coalesce(osl.first_auto_assign_timestamp, from_unixtime(oct.submit_time - 3600)) inflow_timestamp
                ,coalesce(date_diff('second',from_unixtime(oct.submit_time - 3600),osl.last_incharge_timestamp)/cast(60 as double),0) as lt_incharged
                ,coalesce(date_diff('second',from_unixtime(oct.submit_time - 3600),from_unixtime(oct.final_delivered_time - 3600))/cast(60 as double),0) as lt_completion
                
                ,coalesce(date_diff('second',osl.last_incharge_timestamp,osl.last_picked_timestamp)/cast(60 as double),0) as lt_pickup_time
                ,CASE WHEN coalesce(ocs.prepare_time_actual,0) > 0 then ocs.prepare_time_actual/cast(60 as double) 
                      else date_diff('second',osl.first_confirmed_timestamp,osl.last_picked_timestamp)/cast(60 as double) end as lt_prepare_time
                -- ,coalesce(ocs.prepare_time_actual,date_diff('second',osl.first_confirmed_timestamp,osl.last_picked_timestamp))/cast(60 as double) as lt_prepare_time
                ,coalesce(date_diff('second',osl.last_picked_timestamp,osl.last_delivered_timestamp)/cast(60 as double),0) as lt_pickup_delivered

            ,case 
                when oct.cancel_type in (0,5) and oct.status = 8 then 'System'
                when oct.cancel_type = 1 and oct.status = 8 then 'CS BPO'
                when oct.cancel_type = 2 and oct.status = 8 then 'User'
                when oct.cancel_type in (3,4) and oct.status = 8 then 'Merchant'
                when oct.cancel_type = 6 and oct.status = 8 then 'Fraud'
                end as cancel_actor

        from shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct

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

            --mex prep done
        LEFT JOIN shopeefood.foody_order_db__order_completed_merchant_search_tab__reg_daily_s0_live ocs ON oct.id = ocs.id
        -- select * from shopeefood.foody_order_db__order_completed_merchant_search_tab__reg_daily_s0_live
        WHERE 1=1

        -- and date(from_unixtime(oct.submit_time - 3600)) >= current_date - interval '45' day 
                                        AND 
    (date(from_unixtime(oct.submit_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(oct.submit_time - 3600)) = date'2023-03-03'
    )
        and oct.city_id <> 238  
)
,assignment as 
(
    SELECT 
            case when city_id = 217 then 'HCM'
                 when city_id = 218 then 'HN'
                 when city_id = 219 then 'DN'
                 else 'OTH' end as city_group
            ,date(from_unixtime(create_time - 3600)) as created_date 
            ,HOUR(from_unixtime(create_time - 3600)) as created_hour
            ,order_id
            ,COUNT(order_uid) as total_assign
            ,COUNT(case when status in (8,9,17,18) then order_uid else null end) as total_ignore                   
    
FROM     
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live

        UNION

SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
)
WHERE 1 = 1 
                                        AND 
    (date(from_unixtime(create_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(create_time - 3600)) = date'2023-03-03'
    )
GROUP BY 1,2,3,4     
)
,free_pick as 
(
SELECT base.order_id 
      ,base.order_type
      ,base.order_code
      ,base.city_name
      ,base.city_group
      ,base.assign_type
      ,create_time
      ,update_time
      ,create_hour
      ,minute_range
      ,date_
      ,year_week
      ,shipper_id

from
(SELECT a.order_uid
,a.order_id
,case when a.order_type = 0 then '1. Food/Market'
    when a.order_type in (4,5) then '2. NS'
    when a.order_type = 6 then '3. NSS'
    -- when a.order_type = 3 then '1. Food/Market'
    else 'Others' end as order_type  
,case when a.order_type in (7,200) then 7 else a.order_type end as order_code          
,a.city_id
,city.name_en as city_name
,case when a.city_id  = 217 then '1. HCM'
    when a.city_id  = 218 then '2. HN'
    when a.city_id  = 219 then '3. DN' else '4. OTH' 
    end as city_group
-- ,a.assign_type as at    
,case when a.assign_type = 1 then '1. Single Assign'
      when a.assign_type in (2,4) then '2. Multi Assign'
      when a.assign_type = 3 then '3. Well-Stack Assign'
      when a.assign_type = 5 then '4. Free Pick'
      when a.assign_type = 6 then '5. Manual'
      when a.assign_type in (7,8) then '6. New Stack Assign'
      else null end as assign_type
      
-- ,a.update_time
-- ,a.create_time
,from_unixtime(a.create_time - 60*60) as create_time
,from_unixtime(a.update_time - 60*60) as update_time
,extract(hour from from_unixtime(a.create_time - 60*60)) create_hour
,case when extract(minute from from_unixtime(a.create_time - 60*60)) < 30 then '1. 0 - 30 min'
      when extract(minute from from_unixtime(a.create_time - 60*60)) >= 30 then '2. 30 - 60 min'
      else null end as minute_range
,date(from_unixtime(a.create_time - 60*60)) as date_
,case when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
      when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
        else YEAR(cast(FROM_UNIXTIME(a.create_time - 60*60) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 60*60) as date)) end as year_week
,a.status
,case when a.experiment_group in (3,4,7,8) then 1 else 0 end as is_auto_accepted
,case when a.experiment_group in (5,6,7,8) then 1 else 0 end as is_ca
-- ,sa.total_single_assign_turn
,case when sa.total_single_assign_turn = 0 or sa.total_single_assign_turn is null then '# 0' 
    when sa.total_single_assign_turn = 1 then '# SA 1'
    when sa.total_single_assign_turn = 2 then '# SA 2'
    when sa.total_single_assign_turn = 3 then '# SA 3'
    when sa.total_single_assign_turn > 3 then '# SA 3+'
    else null end as total_single_assign_turn
,a.shipper_uid as shipper_id    
-- ,a_filter.order_id as f_


from (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
        where status in (3,4) -- shipper incharge
                                          AND 
    (date(from_unixtime(create_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(create_time - 3600)) = date'2023-03-03'
    )
        UNION
    
        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
        where status in (3,4) -- shipper incharge
                                        AND 
    (date(from_unixtime(create_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(create_time - 3600)) = date'2023-03-03'
    )
    )a
    
    -- take last incharge
    LEFT JOIN 
            (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
    
            from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
            where status in (3,4) -- shipper incharge
                                        AND 
    (date(from_unixtime(create_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(create_time - 3600)) = date'2023-03-03'
    )
            UNION
        
            SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
    
            from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
            where status in (3,4) -- shipper incharge
                                        AND 
    (date(from_unixtime(create_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(create_time - 3600)) = date'2023-03-03'
    )        
        )a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time
        
    -- auto accept 
    
        
    -- count # single assign for each order 
    LEFT JOIN 
            (SELECT a.order_uid
                ,count(case when assign_type = 1 then a.order_id else null end) as total_single_assign_turn
            
            from
                (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
        
                    from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                WHERE 1 = 1 
                                        AND 
    (date(from_unixtime(create_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(create_time - 3600)) = date'2023-03-03'
    )
                UNION
            
                SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
        
                    from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                WHERE 1 = 1 
                                        AND 
    (date(from_unixtime(create_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(create_time - 3600)) = date'2023-03-03'
    ) 
                )a
                
                GROUP By 1
            )sa on sa.order_uid = a.order_uid
            
    -- location
    left join (SELECT * from shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live
                   )city on city.id = a.city_id and city.country_id = 86
    
        
where 1=1
and a_filter.order_id is null -- take last incharge
-- and a.order_id = 109630183

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

-- LIMIT 100
    
)base    


where base.date_ between date('2021-04-01') and date(now()) - interval '1' day

and order_type = '1. Food/Market'
and assign_type = '4. Free Pick'

)
,

free_pick_order as 
(
 SELECT shipper_uid, order_id
       ,coalesce(max(case when status = 11 then create_time else null end),0) as second_incharge_timestamp

 FROM shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live

 GROUP BY 1,2
)
,
check_maxhandle as 
(
 SELECT shipper_uid
       ,order_id
       ,coalesce(max(case when status = 11 then create_time else null end),9999999999) as first_incharge_timestamp
       ,coalesce(max(case when status = 7 then create_time else null end),0) as first_delivered_timestamp
       ,coalesce(max(case when status = 10 then create_time else null end),9999999999) as first_reassign_timestamp
       ,coalesce(max(case when status = 12 then create_time else null end),9999999999) as first_deny_timestamp
       ,coalesce(max(case when status = 8 then create_time else null end),9999999999) as first_cancel_timestamp
       
 FROM shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live

 GROUP BY 1,2
)

, all_freepick AS
(
SELECT *
      ,case when date_diff('second',second_incharge_timestamp,first_delivered_timestamp) <= 30 then '1. 0 - 30s'
            when date_diff('second',second_incharge_timestamp,first_delivered_timestamp) <= 60 then '2. 30 - 60s'
            when date_diff('second',second_incharge_timestamp,first_delivered_timestamp) <= 300 then '3. 60 - 300s'
            when date_diff('second',second_incharge_timestamp,first_delivered_timestamp) > 300 then '4. 300s++'
            else null end as diff_2nd_incharge_1st_del_group
FROM
(
SELECT a.order_id as second_order_id
      ,a.order_type
      ,a.order_code
      ,a.city_name
      ,a.city_group
      ,a.assign_type
      ,a.create_time
      ,a.update_time
      ,a.create_hour
      ,a.minute_range
      ,a.date_
      ,a.year_week
      ,a.shipper_id
      ,from_unixtime(b.second_incharge_timestamp - 60*60) as second_incharge_timestamp
      ,c.order_id as first_order_id
     ,from_unixtime(c.first_incharge_timestamp - 60*60) first_incharge_timestamp
     ,from_unixtime(c.first_delivered_timestamp - 60*60) first_delivered_timestamp
     ,from_unixtime(c.first_reassign_timestamp - 60*60) first_reassign_timestamp
     ,from_unixtime(c.first_deny_timestamp - 60*60) first_deny_timestamp
     ,from_unixtime(c.first_cancel_timestamp - 60*60) first_cancel_timestamp
     ,row_number() over(partition by a.order_id order by from_unixtime(c.first_incharge_timestamp - 60*60) asc) rank
FROM (select * from free_pick) a

LEFT JOIN (select * from free_pick_order) b on a.order_id = b.order_id and a.shipper_id = b.shipper_uid

LEFT JOIN (select * from check_maxhandle) c on b.shipper_uid = c.shipper_uid and ((b.second_incharge_timestamp > c.first_incharge_timestamp and b.second_incharge_timestamp < c.first_delivered_timestamp)
                                                                  and (b.second_incharge_timestamp < c.first_reassign_timestamp) 
                                                                  and (b.second_incharge_timestamp < c.first_deny_timestamp) 
                                                                  and (b.second_incharge_timestamp < c.first_cancel_timestamp) 
                                                                    )
WHERE b.second_incharge_timestamp > 0        
and c.order_id > 0
)base

where 1=1
and rank = 1
--group by 1
)
,final as 
(select 
         od.*
        ,case when cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) = 2 then 1 else 0 end as is_inshift
        ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 then 1 
              when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 2
              else 0 end as  is_stack_group
        ,case when b.second_order_id is not null and b.first_order_id is not null then 1 else 0 end as is_fp_max_handle_2nd_order 
        ,case when c.first_order_id is not null then 1 else 0 end as is_fp_max_handle_1st_order
        ,least(1,(case when b.second_order_id is not null and b.first_order_id is not null then 1 else 0 end) + (case when c.first_order_id is not null then 1 else 0 end)) is_fp_maxhandle               
        ,sa.total_assign
        ,sa.total_ignore

from order_raw od 

LEFT JOIN assignment sa on sa.order_id = od.id 

LEFT JOIN all_freepick b on od.id = b.second_order_id 

LEFT JOIN (SELECT distinct first_order_id FROM all_freepick) c on od.id = c.first_order_id

left join (select id,ref_order_id,ref_order_category,group_id 
        from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
     on dot.ref_order_id = od.id and dot.ref_order_category = 0

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet 
     on dot.id = dotet.order_id

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
                                    ,from_unixtime(a.create_time - 60*60) as create_time
                                    ,from_unixtime(a.update_time - 60*60) as update_time
                                    ,date(from_unixtime(a.create_time - 60*60)) as date_
                                    ,case when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                          when cast(FROM_UNIXTIME(a.create_time - 60*60) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                                            else YEAR(cast(FROM_UNIXTIME(a.create_time - 60*60) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 60*60) as date)) end as year_week
                                    ,ogm.total_order_in_group
                            
                            from (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                            
                                    from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                                    where status in (3,4) -- shipper incharge
                                        AND 
    (date(from_unixtime(create_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(create_time - 3600)) = date'2023-03-03'
    )
                                    UNION
                                
                                    SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                            
                                    from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                                    where status in (3,4) -- shipper incharge
                                    AND 
    (date(from_unixtime(create_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(create_time - 3600)) = date'2023-03-03'
    )
                                )a
                                
                                -- take last incharge
                                LEFT JOIN 
                                        (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
                                
                                        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                                        where status in (3,4) -- shipper incharge
                                        AND 
    (date(from_unixtime(create_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(create_time - 3600)) = date'2023-03-03'
    )
                                
                                        UNION
                                    
                                        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status
                                
                                        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                                        where status in (3,4) -- shipper incharge
                                        AND 
    (date(from_unixtime(create_time - 3600)) = date'2023-03-08'
    or 
     date(from_unixtime(create_time - 3600)) = date'2023-03-03'
    )
                                    )a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time
                               LEFT JOIN
                               (select 
                                                group_id
                                            --    ,ref_order_category 
                                            --    ,ref_order_code
                                            --    ,mapping_status
                                               ,count(distinct ref_order_code) as total_order_in_group
                               
                               
                               from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day)
                               group by 1 
                               )  ogm on ogm.group_id =  (case when a.order_type = 200 then a.order_id else 0 end)    
                                    
                                -- auto accept 
                                
                               LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end
                            
                            where 1=1
                            and a_filter.order_id is null -- take last incharge
                            -- and a.order_id = 9490679
                            and a.order_type = 200
                            
                            GROUP BY 1,2,3,4,5,6,7,8,9
                            
)group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category 
)
SELECT 
         f.created_date
        ,HOUR(f.created_timestamp)
        ,f.city_group
        ,f.foody_service
        ,is_inshift
        ,f.is_stack_group as is_stack_group_1group_2stack
        ,f.is_fp_maxhandle
        ,f.item_range
        ,count(f.id) as total_gross
        ,count(case when f.order_status = 'Delivered' then f.id else null end) as total_net 
        ,count(case when f.order_status = 'Cancelled' and cancel_reason = 'No driver' then f.id else null end) as total_cnd 
        
        ,sum(f.total_assign) as total_assign 
        ,sum(f.total_ignore) as total_ignore
        ,sum(case when is_asap = 1 and f.order_status = 'Delivered' then lt_completion else null end) as lt_e2e
        ,sum(case when is_asap = 1 and f.order_status = 'Delivered' then lt_incharged else null end) as lt_incharged
        ,sum(case when is_asap = 1 and f.order_status = 'Delivered' then lt_pickup_time else null end) as lt_pickup_time
        ,sum(case when is_asap = 1 and f.order_status = 'Delivered' then lt_pickup_time else null end) as lt_pickup_time
        ,sum(case when is_asap = 1 and f.order_status = 'Delivered' then lt_prepare_time else null end) as lt_prepare_time
        ,sum(case when f.order_status = 'Delivered' then distance else null end) as distance
        ,count(case when is_asap = 1 and f.order_status = 'Delivered' then f.id else null end) as total_net_asap


FROM final f 

WHERE (f.created_date = date'2023-03-08'
       or 
       f.created_date = date'2023-03-03' 
)

GROUP BY 1,2,3,4,5,6,7,8