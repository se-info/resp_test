-- DROP TABLE IF EXISTS dev_vnfdbi_opsndrivers.phong_raw_order_checking;
-- CREATE TABLE IF NOT EXISTS dev_vnfdbi_opsndrivers.phong_raw_order_checking as 

select 
        * 

from 
(select 
       dot.ref_order_code
      ,dot.uid as shipper_id
      ,sm.shopee_uid
      ,sm.full_name
      ,city.name_en as pick_city_name      
      ,fa.last_incharge_timestamp
      ,fa.last_picked_timestamp 
      ,from_unixtime(dot.real_drop_time - 3600) as final_status_timestamp
      ,date(from_unixtime(dot.real_drop_time - 3600)) as final_status_date  
      ,case when dot.ref_order_category = 0 then 'order_delivery'
            when dot.ref_order_category = 3 then 'now_moto'
            when dot.ref_order_category = 4 then 'now_ship'
            when dot.ref_order_category = 5 then 'now_ship'
            when dot.ref_order_category = 6 then 'now_ship_shopee'
            when dot.ref_order_category = 7 then 'now_ship_sameday'
            else null end as source
      ,case when dot.order_status = 400 then 'Delivered'
            when dot.order_status = 401 then 'Quit'
            when dot.order_status in (402,403,404) then 'Cancelled'
            when dot.order_status in (405) then 'Returned'
            else 'Others' end as order_status

      ,delivery_cost*1.00/100 as driver_shipping_fee
    --   ,case when receiver_payment_method
      ,sender_payment*1.00/100 as payment_at_seller
      ,receiver_payment*1.00/100 as payment_at_buyer
      ,case when dot.ref_order_category <> 0 then nss.item_value else oct.sub_total*1.00/100 end as item_value                      
      ,case when dot.ref_order_category <> 0 then coalesce(nss.return_fee,0) else 0 end as return_fee
      ,case when dot.ref_order_category <> 0 then nss.seller_name else mm.merchant_name end as seller_name  
        

from (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval'1' day)  dot 

LEFT JOIN 
(SELECT id,concat('now_ship_',cast(id as VARCHAR)) as uid,booking_type,code as order_code,shipper_id, distance,create_time, status, payment_method,'now_ship' as original_source,city_id,cast(json_extract(extra_data,'$.pick_address_info.district_id') as DOUBLE) as district_id,item_value*1.00/100 as item_value,cast(json_extract(extra_data,'$.shipping_fee.return_fee') as bigint) as return_fee
        ,cast(json_extract(extra_data,'$.sender_info.name') as varchar ) as seller_name 

        from shopeefood.foody_express_db__booking_tab__reg_continuous_s0_live

    UNION

SELECT id,concat('now_ship_shopee_',cast(id as VARCHAR)) as uid, 4 as booking_type,code as order_code, shipper_id,distance,create_time,status,1 as payment_method,'now_ship_shopee' as original_source,city_id,cast(json_extract(extra_data,'$.sender_info.district_id') as DOUBLE) as district_id,item_value*1.00/100 as item_value,cast(json_extract(extra_data,'$.shipping_fee.return_fee') as bigint) as return_fee
        ,cast(json_extract(extra_data,'$.sender_info.name') as varchar ) as seller_name 

        from shopeefood.foody_express_db__shopee_booking_tab__reg_daily_s0_live
) nss on nss.id = dot.ref_order_id and nss.original_source = (case when dot.ref_order_category = 0 then 'order_delivery'
                                                                when dot.ref_order_category = 3 then 'now_moto'
                                                                when dot.ref_order_category = 4 then 'now_ship'
                                                                when dot.ref_order_category = 5 then 'now_ship'
                                                                when dot.ref_order_category = 6 then 'now_ship_shopee'
                                                                when dot.ref_order_category = 7 then 'now_ship_sameday'
                                                                else null end)


LEFT JOIN shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on oct.id = dot.ref_order_id and dot.ref_order_category = 0 

LEFT JOIN shopeefood.foody_mart__profile_merchant_master mm on mm.merchant_id = oct.restaurant_id and mm.grass_date = 'current' 

LEFT JOIN shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live sm on sm.uid = dot.uid 

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

                    LEFT JOIN
                    (
                    SELECT   order_id , 0 as order_type
                            ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
                            ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp  
                            ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp 
                            from shopeefood.foody_order_db__order_status_log_tab__reg_continuous_s0_live
                            where 1=1 
                            -- and grass_schema = 'foody_order_db'
                            group by 1,2
                    
                    UNION
                    
                    SELECT   ns.order_id, ns.order_type ,min(from_unixtime(create_time - 60*60)) first_auto_assign_timestamp
                            ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
                            ,max(case when status in (3,4) then cast(from_unixtime(update_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp 
                    FROM 
                            ( SELECT order_id, order_type , create_time , update_time, status
                    
                             from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                             where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                             and grass_schema = 'foody_partner_archive_db'   
                             UNION
                        
                             SELECT order_id, order_type, create_time , update_time, status
                        
                             from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                             where order_type in (4,5,6,7) -- now ship/ns shopee/ ns same day
                             and grass_schema = 'foody_partner_db'
                             )ns
                    GROUP BY 1,2
                    )fa on dot.ref_order_id = fa.order_id and dot.ref_order_category = fa.order_type

where 1 = 1
and date(from_unixtime(dot.real_drop_time - 3600)) between current_date - interval '90' day and current_date - interval '1' day

)

