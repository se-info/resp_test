with base as 
(SELECT
        dot.uid as shipper_id
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
        ,hour(FROM_UNIXTIME(dot.submitted_time- 60*60))*100 + minute(FROM_UNIXTIME(dot.submitted_time- 60*60)) as hour_min

        ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
            when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(FROM_UNIXTIME(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
            else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end as report_date
        ,date(FROM_UNIXTIME(dot.submitted_time- 60*60)) created_date
        ,HOUR(FROM_UNIXTIME(dot.submitted_time- 60*60)) AS created_hour
        ,case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 60*60) end as last_delivered_timestamp
                                                                                                    
        ,case when dot.pick_city_id = 217 then 'HCM City'
            when dot.pick_city_id = 218 then 'Ha Noi City'
            when dot.pick_city_id = 219 then 'DN'
            ELSE 'OTH' end as city_group
    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
    -- LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on dot.pick_city_id = city.id and city.country_id = 86
    WHERE dot.ref_order_category = 0
    )

select 
        created_date
       ,created_hour 
       ,city_group
       ,count(distinct order_id) as gross_orders
       ,count(distinct case when order_status = 400 then order_id else null end) as net_orders



from base 

where 1 = 1 

and 
(
      created_date = date'2022-08-26' and hour_min between 1030 and 1230
      or 
      created_date = date'2022-08-27' and hour_min between 1030 and 1230  
      or 
      created_date = date'2022-08-27' and hour_min between 1700 and 1900  
)

group by 1,2,3

