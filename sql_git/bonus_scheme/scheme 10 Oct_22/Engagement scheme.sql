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
              ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                    when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                    else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
              ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then hour(from_unixtime(dot.real_drop_time - 60*60))*100+minute(from_unixtime(dot.real_drop_time - 60*60)) 
                    when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then hour(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))*100 + minute(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                    else hour(from_unixtime(dot.submitted_time- 60*60))*100 + minute(from_unixtime(dot.submitted_time- 60*60)) end as report_hour_min                      
              ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
              ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
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
        and (case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                    when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                    else date(from_unixtime(dot.submitted_time- 60*60)) end) between date '2020-12-01' and current_date - interval '1' day
        -- and dot.pick_city_id not in (0,238,468,469,470,471,472,227,269) -- 227: Bac Giag, 269 Tay Ninh
        
)
select 
        a.shipper_id
       ,a.report_date   
       ,sp.shopee_uid 
       ,sm.shipper_name
       ,sm.city_name 
       ,case when sm.shipper_type_id = 12 then 'Hub' else 'Non hub' end as working_type
       ,case when sp.take_order_status = 1 then 'Normal' 
            when sp.take_order_status = 2 then 'Stop'
            else 'Pending' end as order_status
       ,case when sm.shipper_status_code = 1 then 'Normal' else 'Off' end as working_status 
       ,sp.main_phone    
       ,count(distinct order_code) as total_orders
    --    ,count(distinct case when report_hour_min <= 1009 then order_code else null end) as total_order_before_10_09min
    --    ,count(distinct case when report_hour_min between 1700 and 1900 then order_code else null end) as total_order_17_19

from transacting_driver a 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id  = a.shipper_id and try_cast(sm.grass_date as date) = a.report_date

left join shopeefood.foody_internal_db__shipper_profile_tab__reg_daily_s0_live sp on sp.uid = a.shipper_id


where a.report_date = date'2022-10-10'
and a.shipper_id in 
(21447538
,23093248
,22447814
,21825428
,23061400
,21563420
,23174828
,23069073
,21721856
,40008990
,15267922
,20828386
,21946378
,12714283
,23138055
,20821862
,23093220
,9699742
,40051012
,4419215
,16244664
,23161095
,3685935
,40003487
,22566074
,11135779
,22379463
,23118382
,23107381
,23143888
,23169806
,23090021
,18736493
,40058179
,20792658
,7060499
,40046551
,22811344)
group by 1,2,3,4,5,6,7,8,9


