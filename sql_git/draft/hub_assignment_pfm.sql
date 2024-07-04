with base_dod as
(
SELECT
      dod.uid AS shipper_id
    , city.name_en as city_name 
    , DATE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_date
    , dot.ref_order_id
    , dot.ref_order_code
    , CASE
        WHEN dot.ref_order_category = 0 THEN 'Food/Market'
        --WHEN dot.ref_order_category = 4 THEN 'NS Instant'
        --WHEN dot.ref_order_category = 5 THEN 'NS Food Mex'
        --WHEN dot.ref_order_category = 6 THEN 'NS Shopee'
        --WHEN dot.ref_order_category = 7 THEN 'NS Same Day'
        --WHEN dot.ref_order_category = 8 THEN 'NS Multi Drop'
    ELSE 'SPXI' END AS order_source
    , CASE
        WHEN dod.deny_type = 0 THEN 'NA'
        WHEN dod.deny_type = 1 THEN 'Driver_Fault'
        WHEN dod.deny_type = 10 THEN 'Order_Fault'
        WHEN dod.deny_type = 11 THEN 'Order_Pending'
        WHEN dod.deny_type = 20 THEN 'System_Fault'
    END AS deny_type
    , rea.content_en as deny_reason
    , case when sm.shipper_type_id = 12 
                and slot.uid is not null and (cast(hour(FROM_UNIXTIME(dod.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(dod.create_time - 3600)) as double)/60) between slot.start_time and slot.end_time then 'Hub Inshift'
                else 'Non Hub' end as shipper_type
    ,FROM_UNIXTIME(dod.create_time - 3600) as deny_timestamp
    ,date_format(FROM_UNIXTIME(dod.create_time - 3600),'%T') as hour_timestamp
    ,cast(hour(FROM_UNIXTIME(dod.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(dod.create_time - 3600)) as double)/60 as hour_minute

    FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod

    left  join shopeefood.foody_internal_db__deny_reason_template_tab__reg_daily_s0_live rea on rea.id = dod.reason_id

    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dod.order_id = dot.id
    LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86
    LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dod.uid and try_cast(sm.grass_date as date) =  date(FROM_UNIXTIME(dod.create_time - 3600))
    
    --HUB SHIFT CHECK 
    left  join ( select  uid 
                        ,date(from_unixtime(date_ts - 3600)) as date_ts 
                        ,(start_time*1.0000/3600) as start_time 
                        ,(end_time*1.0000/3600) as end_time
    from 
    shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live

    ) slot 

    on slot.uid = dod.uid and DATE(FROM_UNIXTIME(dod.create_time - 3600)) = date_ts 

    WHERE 1=1

    and dot.ref_order_category <> 0 --NSS service 
    --and reason_id = 118
    
    order by dod.create_time desc
    )

--Assign base 
,base_assign as
(SELECT
    CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
        , order_id, city_id, assign_type, update_time, create_time, status, order_type
        , experiment_group, shipper_uid AS shipper_id

FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
WHERE status in (3,4,8,9,2,14,15,17,18) -- shipper incharge + deny + ignore
AND grass_schema = 'foody_partner_archive_db'

UNION ALL

SELECT
    CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
        , order_id, city_id, assign_type, update_time, create_time, status, order_type
        , experiment_group, shipper_uid AS shipper_id

FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
WHERE status in (3,4,8,9,2,14,15,17,18) -- shipper incharge + deny + ignore
AND schema = 'foody_partner_db'
)

---Ignore/Assign 
,base_assign1 as
(SELECT
    a.order_uid
    , a.order_id
    , CASE
        WHEN a.order_type = 0 THEN '1. Food/Market'
        --WHEN a.order_type in (4,5) THEN '2. NS'
        --WHEN a.order_type = 6 THEN '3. NSS'
        --WHEN a.order_type = 7 THEN '4. NS Same Day'
        ELSE 'SPXI' END AS order_source
    , a.order_type AS order_code
    , a.city_id
    , city.name_en AS city_name
    , case when sm.shipper_type_id = 12 
                and slot.uid is not null and (cast(hour(FROM_UNIXTIME(a.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(a.create_time - 3600)) as double)/60) between slot.start_time and slot.end_time then 'Hub Inshift'
                else 'Non Hub' end as shipper_type
    , CASE
        WHEN a.city_id  = 217 THEN 'HCM'
        WHEN a.city_id  = 218 THEN 'HN'
        WHEN a.city_id  = 219 THEN 'DN'
        WHEN a.city_id  = 220 THEN 'HP'
        ELSE 'OTH'
    END AS city_group
    ,CASE
        WHEN a.assign_type = 1 THEN '1. Single Assign'
        WHEN a.assign_type in (2,4) THEN '2. Multi Assign'
        WHEN a.assign_type = 3 THEN '3. Well-Stack Assign'
        WHEN a.assign_type = 5 THEN '4. Free Pick'
        WHEN a.assign_type = 6 THEN '5. Manual'
        WHEN a.assign_type in (7,8) THEN '6. New Stack Assign'
    ELSE NULL END AS assign_type
    , DATE(FROM_UNIXTIME(a.create_time - 3600)) AS date_
    , CASE
        WHEN WEEK(DATE(from_unixtime(a.create_time - 3600))) >= 52 AND MONTH(DATE(from_unixtime(a.create_time - 3600))) = 1 THEN (YEAR(DATE(from_unixtime(a.create_time - 3600)))-1)*100 + WEEK(DATE(from_unixtime(a.create_time - 3600)))
        WHEN WEEK(DATE(from_unixtime(a.create_time - 3600))) = 1 AND MONTH(DATE(from_unixtime(a.create_time - 3600))) = 12 THEN (YEAR(DATE(from_unixtime(a.create_time - 3600)))+1)*100 + WEEK(DATE(from_unixtime(a.create_time - 3600)))
    ELSE YEAR(DATE(from_unixtime(a.create_time - 3600)))*100 + WEEK(DATE(from_unixtime(a.create_time - 3600))) END AS year_week
    , a.status
    , IF(a.experiment_group IN (3,4,7,8), 1, 0) AS is_auto_accepted
    , a.shipper_id
    ,FROM_UNIXTIME(a.create_time - 3600) as create_timestamp
    ,cast(hour(FROM_UNIXTIME(a.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(a.create_time - 3600)) as double)/60 as hour_minute

FROM base_assign a

LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city ON city.id = a.city_id AND city.country_id = 86

--HUB SHIFT 

left  join ( select  uid 
                        ,date(from_unixtime(date_ts - 3600)) as date_ts 
                        ,(start_time*1.0000/3600) as start_time 
                        ,(end_time*1.0000/3600) as end_time
    from 
    shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live

    ) slot on slot.uid = a.shipper_id and slot.date_ts = DATE(FROM_UNIXTIME(a.create_time - 3600))

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id and try_cast(sm.grass_date as date) = DATE(FROM_UNIXTIME(a.create_time - 3600)) 


WHERE 1=1
--AND DATE(FROM_UNIXTIME(a.create_time - 3600)) = date '2022-04-15'
--and status IN (8,9,17,18)

)


,final as 

---Denied
(select   a.deny_date as date_
        ,a.city_name
        ,a.shipper_type
        ,a.order_source
        ,'denied' as metrics
        ,a.deny_reason as reason  
        --,a.deny_type
        ,count(ref_order_code) as total_order


from base_dod a 

where  1 = 1 
and a.deny_type = 'Driver_Fault'
and a.city_name in ('HCM City','Ha Noi City')

group by 1,2,3,4,5,6


---Ignore
UNION ALL 

select   a.date_ as date_ 
        ,a.city_name
        ,a.shipper_type
        ,a.order_source
        ,'ignored' as metrics
        ,null as reason
        ,count(case when a.status in (8,9,17,18) then a.order_uid else null end) as total_order


from base_assign1 a 

where 1 = 1 
and a.city_name in ('HCM City','Ha Noi City')
and a.order_source = 'SPXI'

group by 1,2,3,4,5,6

---Incharged
UNION ALL 

select   a.date_ as date_ 
        ,a.city_name
        ,a.shipper_type
        ,a.order_source
        ,'incharged' as metrics
        ,null as reason
        ,count(case when a.status in (3,4) then a.order_uid else null end) as total_order


from base_assign1 a 

where 1 = 1 
and a.city_name in ('HCM City','Ha Noi City')
and a.order_source = 'SPXI'

group by 1,2,3,4,5,6
UNION ALL 

select   a.date_ as date_ 
        ,a.city_name
        ,a.shipper_type
        ,a.order_source
        ,'assign' as metrics
        ,null as reason
        ,count(a.order_uid ) as total_order


from base_assign1 a 

where 1 = 1 
and a.city_name in ('HCM City','Ha Noi City')
and a.order_source = 'SPXI'

group by 1,2,3,4,5,6

) 

select  * 


from final 

where date_ between current_date - interval '30' day and current_date - interval '1' day 