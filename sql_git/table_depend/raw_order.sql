drop table if exists dev_vnfdbi_opsndrivers.food_raw_phong;
create table if not exists  dev_vnfdbi_opsndrivers.food_raw_phong as 

/*        SELECT   oct.id
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
                ,di.name_en as district_name
                
                ,case when oct.city_id = 217 then 'HCM'
                    when oct.city_id = 218 then 'HN'
                    when oct.city_id = 219 then 'DN'
                    when oct.city_id = 220 then 'HP'
                    ELSE 'OTH' end as city_group
                
                ,oct.foody_service_id
                 
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
                ,oct.distance
                ,cast(json_extract(oct.extra_data,'$.total_item') as bigint) as total_item
                ,oct.total_amount*1.00/100 as total_amount 
                ,oct.merchant_paid_amount*1.00/100 as merchant_paid
                ,date(osl.last_cancel_timestamp) cancel_date
                ,coalesce(osl.first_auto_assign_timestamp, from_unixtime(oct.submit_time - 3600)) inflow_timestamp
                ,date_format(from_unixtime(cfm.confirm_time -3600),'%H:%i:%S') as confirm_time---cfm.confirm_time
                ,date_format(from_unixtime(pick.pick_time -3600),'%H:%i:%S') as pick_time---pick.pick_time
                ,(pick.pick_time - cfm.confirm_time) / 60 as prep_time  

        from shopeefood.foody_order_db__order_completed_tab__reg_daily_s0_live oct

        left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86
        left join shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = oct.district_id 
        left join shopeefood.foody_mart__fact_gross_order_join_detail go on go.id = oct.id
        left join shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr on cr.id = try_cast(json_extract_scalar(oct.extra_data,'$.note_ids') as int) -- note_ids: cancel_reason

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

        left join (select order_id
                                ,create_time as "confirm_time"
                                from (
                                select
                                order_id
                                ,create_time
                                ,row_number() over (partition by order_id order by create_time asc) as "rank"
                                from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                                where status = 13 --- confirm ----
                                group by 1,2
                                )
                                
                        where rank = 1        
                    ) cfm on cfm.order_id = oct.id
        left join ( select order_id
                                ,create_time as "pick_time"
                                from (
                                        select
                                        order_id
                                        ,create_time
                                        ,row_number() over (partition by order_id order by create_time asc) as "rank"
                                        from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
                                        where status = 6 --- pick ----
                                    )
                                where rank = 1    
                                --group by 1
                    ) pick on pick.order_id = oct.id




        WHERE 1=1

        and cast(go.grass_date as date) between current_date - interval '45' day and current_date  - interval  '1' day
        and go.grass_region = 'VN'
        and date(from_unixtime(oct.submit_time - 3600)) between current_date - interval '45' day and current_date  - interval  '1' day
      --  and oct.foody_service_id = 1
        and oct.city_id <> 238 */
with raw as 
(    SELECT   
         base1.id
        ,base1.uid
        ,base1.cancel_date
        ,base1.created_date
        ,base1.created_hour
        ,base1.shipper_id
        ,base1.city_group
        ,base1.city_name
        ,base1.is_asap
        ,base1.order_status
        ,0 as order_type
        ,coalesce(cancel_by,null) as cancel_by
        ,case when trim(base1.cancel_reason) = 'Shop closed' then (case when po.is_pre_order> 0 then 'Pre-order' else 'Shop closed' end)
              else base1.cancel_reason end as cancel_reason

FROM
(
SELECT
         base.id
        ,base.uid
        ,base.cancel_date
        ,base.created_date
        ,base.created_hour
        ,date(inflow_timestamp) inflow_date
        ,extract(hour from inflow_timestamp) as inflow_hour
        ,base.shipper_id
        ,base.city_group
        ,base.city_name
        ,base.is_asap
        ,base.order_status
        ,base.foody_service
        ,base.cancel_reason
        ,case when base.cancel_reason is null then null
              when base.cancel_reason in ('No driver') then 'No Driver'
              when base.cancel_reason in ('Out of stock', 'Shop closed','Shop busy','Shop did not confirm','Wrong price') then 'Merchant'
              when base.cancel_reason in ('Pending status from bank','Payment failed') then 'System'
            --   when base.cancel_reason in ('Payment failed') then 'Buyer System'
              when base.cancel_reason in ('Affected by quarantine area','Order limit due to Covid') then 'Others'
              else 'User' end as cancel_by        
FROM
        (-- order delivery: Food/Market
        SELECT   oct.id
                ,concat('order_delivery_',cast(oct.id as VARCHAR)) as uid
                ,oct.shipper_uid as shipper_id
                ,from_unixtime(oct.submit_time - 3600) as created_timestamp
                ,date(from_unixtime(oct.submit_time - 3600)) as created_date
                ,extract(hour from from_unixtime(oct.submit_time - 3600)) as created_hour
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
        from shopeefood.foody_order_db__order_completed_tab__reg_daily_s0_live oct
        left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = oct.city_id and city.country_id = 86

        left join shopeefood.foody_mart__fact_gross_order_join_detail go on go.id = oct.id
        left join shopeefood.foody_delivery_admin_db__delivery_note_tab__reg_daily_s0_live cr on cr.id = try_cast(json_extract_scalar(oct.extra_data,'$.note_ids') as int) -- note_ids: cancel_reason
        -- assign time: request archive log
        left join
            (SELECT order_id
                ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                ,max(case when status = 7 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_delivered_timestamp
                ,min(case when status = 13 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_confirmed_timestamp
                ,max(case when status = 8 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_cancel_timestamp
       from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live 
            where 1=1
            -- and order_id = 335026834
            group by order_id
            )osl on osl.order_id = oct.id
        WHERE 1=1

        and go.grass_date != '1970-01-01' and go.grass_region = 'VN'
      --  and oct.foody_service_id = 1d
        and oct.city_id <> 238
        -- and oct.id = 335026834
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

                    from shopeefood.foody_order_db__order_completed_tab__reg_daily_s0_live oct
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


UNION ALL 

    SELECT 
             base.id 
            ,base.uid 
            ,base.cancel_date
            ,base.created_date
            ,base.created_hour
            ,base.shipper_id
            ,base.city_group
            ,base.city_name
            ,base.is_asap
            ,base.order_status
            ,base.order_type
            ,coalesce(cancel_by,null) as cancel_by 
            ,coalesce(cancel_reason,null) as cancel_reason



        -- ,case when base.order_status = 'Delivered' then 1 else 0 end as is_del
        -- ,case when base.order_status = 'Cancelled' then 1 else 0 end as is_cancel
        -- ,case when base.order_status = 'Pickup Failed' then 1 else 0 end as is_pick_failed
        -- ,case when base.order_status = 'Returned' then 1 else 0 end as is_return
        -- ,case when base.order_status = 'Assigning Timeout' then 1 else 0 end as is_assign_timeout
        -- ,case when source = 'now_ship_shopee' then case when base.order_status = 'Assigning Timeout' then 1 else 0 end
        --         when source in ('now_ship_user','now_ship_merchant') then case when assign.last_incharge_timestamp is null and base.assigning_count > 0 and cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') then 1 else 0 end
        --         when source in ('now_ship_same_day') then case when assign.last_incharge_timestamp is null and cancel_reason in ('No Reason','Wait for assigning driver too long','Unable to contact driver','Shipper denied Auto Accept order','Mistaken accept') then 1 else 0 end
        --         else 0 end as is_no_driver_assign
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
            -- ,extract(hour from from_unixtime(ns.create_time - 3600) ) as created_hour
            ,from_unixtime(ns.create_time - 3600) as created_timestamp
            ,from_unixtime(ns.create_time - 3600) as inflow_date
            ,extract(hour from from_unixtime(ns.create_time - 3600)) as created_hour
            ,nsc.created_timestamp as canceled_timestamp
            ,date(nsc.created_timestamp) as cancel_date
            ,cast(from_unixtime(ns.create_time - 3600) as date) as created_date
            ,case when status in (11) and ns.drop_real_time > 0 then date(from_unixtime(ns.drop_real_time - 3600))
                when status in (14) and ns.update_time > 0 then date(from_unixtime(ns.update_time - 3600))
                else date(from_unixtime(ns.create_time- 3600)) end as report_date
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
                when ns.status = 9 then 'No Driver'
                when ns.status = 12 then 'System'
                when ns.status = 3 then 'No Driver'
                else null end as cancel_by
            -- location
            ,case when ns.city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
            ,case when ns.city_id = 217 then 'HCM'
                    when ns.city_id = 218 then 'HN'
                    when ns.city_id = 219 then 'DN'
                    ELSE 'OTH' end as city_group
            ,ns.city_id
            ,case when ns.pick_type = 1 then 1 else 0 end as is_asap
            ,ns.assigning_count

            ,case when ns.status in (6,9,12) then
                    case when ns.booking_type <> 4 and ns.status = 12 and nsc.cancel_type is null then 'SYSTEM_CANCELLED'
                        when ns.booking_type = 4 and nsc.cancel_type is null then 'SYSTEM_CANCELLED'
                        else nsc.cancel_type end
                else null end as cancel_type

            ,case when ns.status in (6,9,12) then coalesce(nsc.cancel_reason,'No Reason') else null end as cancel_reason
            ,case when ns.status = 17 and ns.booking_type = 4 then pf_reason.pick_failed_reason else null end as pick_failed_reason
            from
                    (SELECT id,concat('now_ship_',cast(id as VARCHAR)) as uid, code, booking_type, case when booking_type = 3 then cast(referal_id as varchar) else cast(customer_id as varchar) end as customer_id, shipper_id, distance,create_time, status, payment_method,'now_ship' as original_source,city_id,cast(json_extract(extra_data,'$.pick_address_info.district_id') as DOUBLE) as district_id
                            ,booking_service_type, pick_real_time, drop_real_time, pick_type, update_time, '' as shopee_order_code,assigning_count
                            ,cast(json_extract(extra_data, '$.pick_address_info.address') as varchar) as sender_address
                            ,cast(json_extract(extra_data, '$.sender_info.name')as varchar) as sender_name
                            ,cast(json_extract(extra_data, '$.sender_info.phone')as varchar) as sender_phone

                            ,cast(json_extract(extra_data, '$.drop_address_info.address') as varchar) as receiver_address
                            ,cast(json_extract(extra_data, '$.receiver_info.name')as varchar) as receiver_name
                            ,cast(json_extract(extra_data, '$.receiver_info.phone')as varchar) as receiver_phone


                        from shopeefood.foody_express_db__booking_tab__reg_daily_s0_live

                    UNION ALL

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

            WHERE 1=1

            and ns.city_id <> 238
            --and date(from_unixtime(ns.create_time - 3600)) between DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month and current_date - interval '1' day
            )base

    LEFT JOIN
            (SELECT dot.*, dotet.order_data, group_info.create_time as order_create_time, group_info.group_create_time

             FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot
             left join shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet on dot.id = dotet.order_id

             LEFT JOIN
                    (
                    SELECT ogm.*, ogi.create_time as group_create_time
                    FROM shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm
                    LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi on ogi.id = ogm.group_id
                    WHERE 1=1
                    )group_info on group_info.order_id = dot.id and group_info.mapping_status = 11 and group_info.group_id = dot.group_id
              WHERE dot.grass_schema = 'foody_partner_db'
            ) dot on dot.ref_order_id = base.id and dot.ref_order_code = base.order_code
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

                -- auto accept

            LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end

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
            LEFT JOIN (select id, ref_order_category from shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live group by 1,2) ogi on ogi.id > 0 and ogi.id = case when ns.order_type = 200 then ns.order_id else 0 end

            LEFT JOIN
                        (SELECT ogm.group_id
                            ,ogi.group_code
                            ,count (distinct ogm.ref_order_id) as total_order_in_group
                        FROM shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm
                        LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi on ogi.id = ogm.group_id
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
    WHERE 1=1
    and base.order_status <> 'Others')



    select * from raw 

where created_date between current_date - interval  '30' day and current_date - interval  '1' day 
