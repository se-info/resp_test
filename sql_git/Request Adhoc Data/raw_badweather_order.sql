with assign_raw as 
(SELECT 
        a.order_id
       ,CASE 
            WHEN a.assign_type = 1 then '1. Single Assign'
            WHEN a.assign_type in (2,4) then '2. Multi Assign'
            WHEN a.assign_type = 3 then '3. Well-Stack Assign'
            WHEN a.assign_type = 5 then '4. Free Pick'
            WHEN a.assign_type = 6 then '5. Manual'
            WHEN a.assign_type in (7,8) then '6. New Stack Assign'
            ELSE null END AS assign_type
       ,a.order_type
       ,a.status
       ,CASE WHEN a.order_type = 200 then ogm.ref_order_category else a.order_type END AS order_category 
       ,ogm.ref_order_code
       ,ogi.group_code
       ,CASE WHEN a.order_type = 200 then ogm.ref_order_id else a.order_id END AS ref_order_id
       ,FROM_UNIXTIME(a.create_time - 3600) as assign_timestamp
       ,DATE(FROM_UNIXTIME(a.create_time - 3600)) as assign_date 


FROM 
(SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live

        UNION
    
        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group
                  ,shipper_uid
        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
    )a

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm 
    on ogm.group_id = (CASE WHEN a.order_type = 200 then a.order_id else 0 end)  

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi 
    on ogi.id = (CASE WHEN a.order_type = 200 then a.order_id else 0 end)

WHERE 1 = 1 
AND (CASE WHEN a.order_type = 200 then ogm.ref_order_category else a.order_type end) = 0
)
,preptime as 
(SELECT 
        oct.restaurant_id
       ,oct.id
       ,oct.restaurant_id as merchant_id      
       ,date_diff('second',osl.first_confirmed_timestamp,osl.last_picked_timestamp)/cast(60 as double) as lt_prepare_time     

FROM (SELECT * FROM shopeefood.shopeefood_mart_dwd_vn_order_completed_da WHERE DATE(dt) = current_date - interval '1'day) oct

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
WHERE oct.is_asap = 1 
AND oct.status = 7
AND DATE(FROM_UNIXTIME(oct.submit_time - 3600)) BETWEEN DATE'2023-02-02' - INTERVAL '29' day AND DATE'2023-02-02'            
)
SELECT 
       go.id AS order_id 
      ,FROM_UNIXTIME(go.create_timestamp) AS submitted_time 
      ,go.district_name  
      ,go.bad_weather_fee
      ,CASE 
            WHEN oct.status = 7 then 'Delivered'
            WHEN oct.status = 8 then 'Cancelled'
            WHEN oct.status = 9 then 'Quit'
            END AS order_status
      ,CASE WHEN go.cancel_note_detail[1] 
                  in ('No driver','không có driver','Đơn hàng chưa có Tài xế nhận','No Drivers found','Không có tài xế nhận giao hàng','Lack of shipper','I will not wait any longer','Tôi không muốn tiếp tục đợi') 
                  THEN 'No Driver'
            ELSE 'Other' END AS cancel_reason
      ,oct.distance
      ,oct.restaurant_id AS merchant_id
      ,pre.l30d_preptime
      ,COUNT(ar.ref_order_id) as total_assign            

FROM shopeefood.foody_mart__fact_gross_order_join_detail go

LEFT JOIN (SELECT * FROM shopeefood.shopeefood_mart_dwd_vn_order_completed_da WHERE DATE(dt) = current_date - interval '1'day) oct on oct.id = go.id 

LEFT JOIN assign_raw ar on ar.ref_order_id = go.id 

LEFT JOIN (SELECT 
                  merchant_id
                  ,SUM(lt_prepare_time)/CAST(COUNT(id) AS DOUBLE) as l30d_preptime 
            FROM preptime
            GROUP BY 1
            ) pre on pre.merchant_id = oct.restaurant_id

WHERE DATE(FROM_UNIXTIME(go.create_timestamp)) = DATE'2023-02-02'
AND go.bad_weather_fee > 0
AND go.city_name = 'HCM City'

GROUP BY 1,2,3,4,5,6,7,8,9

                      
