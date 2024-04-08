with transacting_driver as
(SELECT        dot.uid as shipper_id
              ,dot.ref_order_id as order_id
              ,dot.ref_order_code as order_code
              ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
              ,dot.ref_order_category
              ,case when dot.ref_order_category = 0 then 'order_delivery'
                    when dot.ref_order_category = 3 then 'now_moto'
                    when dot.ref_order_category = 4 then 'now_ship'
                    when dot.ref_order_category = 5 then 'now_ship'
                    when dot.ref_order_category = 6 then 'now_ship_shopee'
                    when dot.ref_order_category = 7 then 'now_ship_sameday'
                    else null end source
              ,dot.ref_order_status
              ,dot.order_status
              ,case when dot.order_status = 1 then 'Pending'
                    when dot.order_status in (100,101,102) then 'Assigning'
                    when dot.order_status in (200,201,202,203,204) then 'Processing'
                    when dot.order_status in (300,301) then 'Error'
                    when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
                    else null end as order_status_group
              ,dot.is_asap
            --   ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
            --         when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
            --         else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
            --   ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then hour(from_unixtime(dot.real_drop_time - 60*60))*100+minute(from_unixtime(dot.real_drop_time - 60*60)) 
            --         when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then hour(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))*100 + minute(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
            --         else hour(from_unixtime(dot.submitted_time- 60*60))*100 + minute(from_unixtime(dot.submitted_time- 60*60)) end as report_hour_min                      
              ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
              ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
              ,case when dot.real_drop_time = 0 then null else HOUR(from_unixtime(dot.real_drop_time - 60*60))*100 + MINUTE(from_unixtime(dot.real_drop_time - 60*60)) end as report_hour_min              
            --   ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
              ,case when dot.pick_city_id = 217 then 'HCM'
                    when dot.pick_city_id = 218 then 'HN'
                    when dot.pick_city_id = 219 then 'DN'
                    ELSE 'OTH' end as city_group
        FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
        LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet 
            on dot.id = dotet.order_id
        where 1 = 1 
        and dot.order_status = 400
        and (date(from_unixtime(dot.real_drop_time - 3600)) = date'2022-12-12') or (date(from_unixtime(dot.real_drop_time - 3600)) = date'2022-11-11')
        -- and dot.pick_city_id not in (0,238,468,469,470,471,472,227,269) -- 227: Bac Giag, 269 Tay Ninh
        
)
select 
        date(raw.last_delivered_timestamp) as report_date 
       ,raw.shipper_id
       ,spp.shopee_uid
       ,sm.shipper_name
    --    ,spp.main_phone 
      ,case 
        when sm.city_id in (217,218,219) then 'T1'
        when sm.city_id in (222,273,221,230,220,223) then 'T2'
        when sm.city_id in (248,271,257,228,254,265,263) then 'T3'
        end as city_tier
       ,sm.city_name
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non hub' end as shipper_type 
       ,count(distinct case when date(raw.last_delivered_timestamp) = date'2022-12-12' then raw.order_code end) as Dec_12_total_orders 
       ,count(case when date(raw.last_delivered_timestamp) = date'2022-12-12' and report_hour_min between 1100 and 1300 then raw.order_code else null end) as Dec_12_total_del_11_13
       ,count(case when date(raw.last_delivered_timestamp) = date'2022-12-12' and report_hour_min between 1700 and 2000 then raw.order_code else null end) as Dec_12_total_del_17_20
       ,count(distinct case when date(raw.last_delivered_timestamp) = date'2022-11-11' then raw.order_code end) as Nov_11_total_orders 


from transacting_driver raw 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = raw.shipper_id and try_cast(sm.grass_date as date) = date(raw.last_delivered_timestamp)

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live spp on spp.uid = raw.shipper_id

group by 1,2,3,4,5,6,7
