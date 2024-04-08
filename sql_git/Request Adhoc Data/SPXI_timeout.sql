with raw as 
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
AND (CASE WHEN a.order_type = 200 then ogm.ref_order_category else a.order_type end) = 6
)
,summary as 
(SELECT 
         raw.ref_order_id
        ,CASE 
             WHEN raw.order_type = 200 then raw.ref_order_code else dot.ref_order_code end as ref_order_code
        ,raw.order_category
        ,raw.assign_timestamp
        ,raw.assign_date              
        ,raw.assign_type
        ,raw.status
        ,raw.order_type
        ,raw.group_code
        ,dot.order_status
        ,FROM_UNIXTIME(dot.submitted_time - 3600) as submitted_timestamp
        ,city.name_en as city_name
        ,di.name_en as district_name

FROM raw

LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot 
    on dot.ref_order_id = raw.ref_order_id and dot.ref_order_category = raw.order_category

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

LEFT JOIN shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live di on di.id = dot.pick_district_id

WHERE (
       raw.assign_date between date'2023-01-03' and date'2023-01-10'
       or
       raw.assign_date between date'2023-02-13' and date'2023-02-19'
       )

AND dot.order_status = 102
)

SELECT 
         ref_order_code as order_code
        ,COALESCE(group_code,'') as group_code  
        ,city_name
        ,assign_date
        ,HOUR(submitted_timestamp) as create_hour
        ,DATE(submitted_timestamp) as create_date
        ,district_name
        ,city_name
        ,MIN(CASE WHEN status in (3,4) then assign_timestamp else null end) as first_incharged_time 
        ,COUNT(CASE WHEN status in (3,4) then ref_order_code else null end) as cnt_total_incharge


FROM summary 


GROUP BY 1,2,3,4,5,6,7,8