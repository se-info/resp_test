-- assignment condition 
WITH fa as 
(SELECT   
    order_id 
    , 0 as order_type
    ,min(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as first_auto_assign_timestamp
    ,max(case when status = 21 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_auto_assign_timestamp
    ,max(case when status = 11 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_incharge_timestamp
    ,max(case when status = 6 then cast(from_unixtime(create_time) as TIMESTAMP) - interval '1' hour else null end) as last_picked_timestamp
    from shopeefood.foody_order_db__order_status_log_tab__reg_daily_s0_live
    where 1=1
    and grass_schema = 'foody_order_db'
    group by 1,2
)

,assignment AS
(SELECT
    COALESCE(assign.date_, deny.deny_date) AS report_date
    , COALESCE(assign.shipper_id, deny.shipper_id) AS shipper_id
    , COALESCE(assign.order_type,deny.order_source) as order_source
    , COALESCE(assign.create_hour, deny.create_hour) AS create_hour
    , COALESCE(assign.shipper_type,deny.shipper_type) as shipper_type
    , COALESCE(assign.city_name,deny.city_name) as city_name
    , COALESCE(assign.assign_type,deny.assign_type) as assign_type
    , COALESCE(assign.cnt_total_assign_order,0) + COALESCE(deny.cnt_deny_acceptable,0) AS cnt_total_assign_order
    , COALESCE(assign.cnt_total_incharge,0) AS cnt_total_incharge
    , COALESCE(assign.cnt_ignore_total,0) AS cnt_ignore_total
    , COALESCE(deny.cnt_deny_non_acceptable,0) AS cnt_deny_non_acceptable
    , COALESCE(deny.cnt_deny_acceptable,0) AS cnt_deny_acceptable
    , COALESCE(assign.incharge_time,deny.incharge_time) as incharge_time
    FROM
         (SELECT
            date_
            , create_hour
            , shipper_id
            , order_type
            , shipper_type
            , city_name
            , assign_type
            , COUNT(DISTINCT order_uid) AS cnt_total_assign_order
            , COUNT(DISTINCT IF(status IN (3,4), order_uid, NULL)) AS cnt_total_incharge
            , COUNT(DISTINCT IF(status IN (8,9,17,18) , order_uid, NULL)) AS cnt_ignore_total
            , SUM(incharge_time) as incharge_time

    --    select * 
       FROM
            (SELECT
                  a.shipper_id
                , a.order_uid
                , a.order_id
                , city.name_en as city_name 
                , CASE
                    WHEN a.order_type = 0 THEN '1. Food/Market'
                    WHEN a.order_type in (4,5) THEN '2. NS'
                    WHEN a.order_type = 6 THEN '3. NSS'
                    WHEN a.order_type = 7 THEN '4. NS Same Day'
                ELSE 'Others' END AS order_type
                , a.order_type AS order_code
                ,CASE
                    WHEN a.assign_type = 1 THEN '1. Single Assign'
                    WHEN a.assign_type in (2,4) THEN '2. Multi Assign'
                    WHEN a.assign_type = 3 THEN '3. Well-Stack Assign'
                    WHEN a.assign_type = 5 THEN '4. Free Pick'
                    WHEN a.assign_type = 6 THEN '5. Manual'
                    WHEN a.assign_type in (7,8) THEN '6. New Stack Assign'
                ELSE NULL END AS assign_type
                , DATE(FROM_UNIXTIME(a.create_time - 3600)) AS date_
                , FROM_UNIXTIME(a.create_time - 3600) AS create_timestamp
                , HOUR(FROM_UNIXTIME(a.create_time - 3600)) AS create_hour
                , MINUTE(FROM_UNIXTIME(a.create_time - 3600)) AS create_minute
                , a.status
                , IF(a.experiment_group IN (3,4,7,8), 1, 0) AS is_auto_accepted
                , case when sm.shipper_type_id = 12 
                       and slot.uid is not null and (cast(hour(FROM_UNIXTIME(a.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(a.create_time - 3600)) as double)/60) between slot.start_time and slot.end_time then 'Hub Inshift'
                       else 'Non Hub' end as shipper_type
                ,date_diff('second',fa.first_auto_assign_timestamp,fa.last_auto_assign_timestamp)/cast(60 as double) as incharge_time                                        
            FROM
                    (SELECT
                        CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                        , order_id, city_id, assign_type, update_time, create_time, status, order_type
                        , experiment_group, shipper_uid AS shipper_id

                    FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                    WHERE status IN (3,4,8,9) -- shipper incharge + ignore
                    AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN current_date - interval '30' day and current_date - interval '1' day

                    UNION ALL

                    SELECT
                        CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                        , order_id, city_id, assign_type, update_time, create_time, status, order_type
                        , experiment_group, shipper_uid AS shipper_id

                    FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                    WHERE status IN (3,4,8,9) -- shipper incharge + ignore
                    AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN current_date - interval '30' day and current_date - interval '1' day
                    ) a
        
            LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id and try_cast(sm.grass_date as date) = DATE(FROM_UNIXTIME(a.create_time - 3600))

                --HUB SHIFT CHECK 
            LEFT  JOIN ( select  uid 
                                ,date(from_unixtime(date_ts - 3600)) as date_ts 
                                ,(start_time*1.0000/3600) as start_time 
                                ,(end_time*1.0000/3600) as end_time
            from 
            shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live

            ) slot 

            on slot.uid = a.shipper_id and DATE(FROM_UNIXTIME(a.create_time - 3600)) = date_ts

            LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on a.order_id = dot.ref_order_id and a.order_type = dot.ref_order_category            

            left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86            

            left join fa on fa.order_id = a.order_id and a.order_type = fa.order_type
            ) 
        GROUP BY 1,2,3,4,5,6,7
        ) assign

    FULL JOIN

        (SELECT
            deny_date
            , deny_hour as create_hour
            , shipper_id
            , shipper_type
            , city_name
            , order_source
            , assign_type
            , COUNT(ref_order_code) AS cnt_deny_total
            , COUNT(IF(deny_type <> 'Driver_Fault', ref_order_code, NULL)) AS cnt_deny_acceptable
            , COUNT(IF((deny_type = 'Driver_Fault'), ref_order_code, NULL)) AS cnt_deny_non_acceptable
            , SUM(incharge_time) as incharge_time
 FROM
            (SELECT
                dod.uid AS shipper_id
                , DATE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_date
                , FROM_UNIXTIME(dod.create_time - 3600) AS deny_timestamp
                , HOUR(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_hour
                , MINUTE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_minute
                , dot.ref_order_id
                , dot.ref_order_code
                , dot.ref_order_category
                ,CASE
                    WHEN a.assign_type = 1 THEN '1. Single Assign'
                    WHEN a.assign_type in (2,4) THEN '2. Multi Assign'
                    WHEN a.assign_type = 3 THEN '3. Well-Stack Assign'
                    WHEN a.assign_type = 5 THEN '4. Free Pick'
                    WHEN a.assign_type = 6 THEN '5. Manual'
                    WHEN a.assign_type in (7,8) THEN '6. New Stack Assign'
                ELSE NULL END AS assign_type
                , city.name_en as city_name 
                , rea.content_en as deny_reason
                , case when sm.shipper_type_id = 12 
                       and slot.uid is not null and (cast(hour(FROM_UNIXTIME(dod.create_time - 3600)) as double) + cast(minute(FROM_UNIXTIME(dod.create_time - 3600)) as double)/60) between slot.start_time and slot.end_time then 'Hub Inshift'
                       else 'Non Hub' end as shipper_type                
                , CASE
                    WHEN dot.ref_order_category = 0 THEN 'Food/Market'
                    WHEN dot.ref_order_category = 4 THEN 'NS Instant'
                    WHEN dot.ref_order_category = 5 THEN 'NS Food Mex'
                    WHEN dot.ref_order_category = 6 THEN 'NS Shopee'
                    WHEN dot.ref_order_category = 7 THEN 'NS Same Day'
                    WHEN dot.ref_order_category = 8 THEN 'NS Multi Drop'
                ELSE NULL END AS order_source
                , CASE
                    WHEN dod.deny_type = 0 THEN 'NA'
                    WHEN dod.deny_type = 1 THEN 'Driver_Fault'
                    WHEN dod.deny_type = 10 THEN 'Order_Fault'
                    WHEN dod.deny_type = 11 THEN 'Order_Pending'
                    WHEN dod.deny_type = 20 THEN 'System_Fault'
                END AS deny_type
                , reason_text
                ,date_diff('second',fa.first_auto_assign_timestamp,fa.last_auto_assign_timestamp)/cast(60 as double) as incharge_time                                        
                

            FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod

            LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = dod.uid and try_cast(sm.grass_date as date) = DATE(FROM_UNIXTIME(dod.create_time - 3600))

                --HUB SHIFT CHECK 
            LEFT  JOIN ( select  uid 
                                ,date(from_unixtime(date_ts - 3600)) as date_ts 
                                ,(start_time*1.0000/3600) as start_time 
                                ,(end_time*1.0000/3600) as end_time
            from 
            shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live

            ) slot 

            on slot.uid = dod.uid and DATE(FROM_UNIXTIME(dod.create_time - 3600)) = date_ts 

            left  join shopeefood.foody_internal_db__deny_reason_template_tab__reg_daily_s0_live rea on rea.id = dod.reason_id

            LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dod.order_id = dot.id            

            left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

            left join fa on fa.order_id = dot.ref_order_id and dot.ref_order_category = fa.order_type

            left join 
            (SELECT
                        CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                        , order_id, city_id, assign_type, update_time, create_time, status, order_type
                        , experiment_group, shipper_uid AS shipper_id

                    FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                    WHERE status IN (2,14,15,7) --shipper denied & not received push
                    AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN current_date - interval '45' day and current_date - interval '1' day

                    UNION ALL

                    SELECT
                        CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                        , order_id, city_id, assign_type, update_time, create_time, status, order_type
                        , experiment_group, shipper_uid AS shipper_id

                    FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                    WHERE status IN (2,14,15,7) --shipper denied & not received push
                    AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN current_date - interval '45' day and current_date - interval '1' day
                    ) a on a.order_id = dot.ref_order_id and a.order_type = dot.ref_order_category and dod.uid = a.shipper_id

            WHERE DATE(FROM_UNIXTIME(dod.create_time - 3600)) BETWEEN current_date - interval '30' day and current_date - interval '1' day

            

            ) dod 
        
        group by 1,2,3,4,5,6,7
        ) deny on assign.date_ = deny.deny_date AND assign.shipper_id = deny.shipper_id and assign.shipper_type = deny.shipper_type
) 

-- select * from assignment where assign_type is null and report_date between current_date - interval '30' day and current_date - interval '1' day



select 
       report_date
    --   ,create_hour
      ,order_source
      ,city_name
      ,shipper_type
      ,assign_type
      ,sum(cnt_total_assign_order) as cnt_total_assign_order
      ,sum(cnt_total_incharge) as cnt_total_incharge
      ,sum(cnt_ignore_total) as cnt_ignore_total
      ,sum(cnt_deny_acceptable) as cnt_deny_acceptable
      ,sum(cnt_deny_non_acceptable) as cnt_deny_non_acceptable
    --   ,sum(incharge_time) as incharge_time





from assignment




where report_date between current_date - interval '30' day and current_date - interval '1' day

and (city_name not like '%Test%' and city_name != 'Dien Bien' and city_name is not null)

and order_source = '1. Food/Market'

and shipper_type = 'Hub Inshift'

group by 1,2,3,4,5