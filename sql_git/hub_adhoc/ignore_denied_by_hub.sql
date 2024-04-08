-- assignment condition 
WITH assignment AS
(SELECT * 
        ,row_number()over(partition by order_id order by timestamp asc) as rank


from
        (SELECT
              date_
            , shipper_id
            , 'Ignore' as issue_category
            , order_id
            , order_type
            , order_code
            , create_timestamp as timestamp

        FROM
            (SELECT
                a.shipper_id
                , a.order_uid
                , a.order_id
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
            FROM
                (SELECT
                    CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                    , order_id, city_id, assign_type, update_time, create_time, status, order_type
                    , experiment_group, shipper_uid AS shipper_id

                FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                WHERE status IN (8,9) -- shipper incharge + ignore
                AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN current_date - interval '30' day and current_date - interval '1' day

                UNION ALL

                SELECT
                    CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                    , order_id, city_id, assign_type, update_time, create_time, status, order_type
                    , experiment_group, shipper_uid AS shipper_id

                FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                WHERE status IN (8,9) -- shipper incharge + ignore
                AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN current_date - interval '30' day and current_date - interval '1' day
                ) a
            )

            -- where assign_type != '6. New Stack Assign'
        -- GROUP BY 1,2
        ) assign

UNION ALL

        (SELECT
            deny_date as date_
            , shipper_id
            , concat('Denied',' - ',deny_reason) as issue_category
            , ref_order_id as order_id
            , order_source as order_type
            , ref_order_category as order_code
            , deny_timestamp as timestamp
            ,row_number()over(partition by ref_order_id order by deny_timestamp asc) as rank
            
            -- , COUNT(ref_order_code) AS cnt_deny_total
            -- , COUNT(IF(deny_type <> 'Driver_Fault', ref_order_code, NULL)) AS cnt_deny_acceptable
            -- , COUNT(IF((deny_type = 'Driver_Fault' or deny_reason = 'Did not accept order belongs type "Auto accept"'), ref_order_code, NULL)) AS cnt_deny_non_acceptable
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
                , rea.content_en as deny_reason
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

            FROM shopeefood.foody_partner_db__driver_order_deny_log_tab__reg_daily_s0_live dod
            LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dod.order_id = dot.id

            left  join shopeefood.foody_internal_db__deny_reason_template_tab__reg_daily_s0_live rea on rea.id = dod.reason_id

            WHERE DATE(FROM_UNIXTIME(dod.create_time - 3600)) BETWEEN current_date - interval '30' day and current_date - interval '1' day

            ) dod
) 

)
,final as 
(select 
         a.date_ 
        ,a.shipper_id
        ,a.order_id
        ,sm.shipper_name
        ,city.name_en as city_name
        ,case when sm.shipper_type_id = 12 
                and slot.uid is not null and (cast(hour(a.timestamp) as double) + cast(minute(a.timestamp) as double)/60) between slot.start_time and slot.end_time then 'Hub Inshift'
                else 'Non Hub' end as denied_ignore_working_type
        ,dot.ref_order_code as order_code
        ,case when smm.shipper_type_id = 12 then 'Hub' else 'Non Hub' end as final_status_working_type
        ,a.order_type
        ,a.issue_category
        ,a.timestamp
        ,a.rank



from assignment a 

left join shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.shipper_id and try_cast(sm.grass_date as date) = a.date_

left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot on dot.ref_order_id = a.order_id and a.order_code = dot.ref_order_category

left join shopeefood.foody_mart__profile_shipper_master smm on smm.shipper_id = dot.uid and try_cast(smm.grass_date as date) = date(from_unixtime(dot.real_drop_time - 3600))

left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

--HUB SHIFT CHECK 
    left  join ( select  uid 
                        ,date(from_unixtime(date_ts - 3600)) as date_ts 
                        ,(start_time*1.0000/3600) as start_time 
                        ,(end_time*1.0000/3600) as end_time
    from 
    shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live

    ) slot 

    on slot.uid = a.shipper_id and a.date_ = date_ts 

where date_ between current_date - interval '14' day and current_date - interval '1' day

-- and dot.order_status

)
-- select  
--         date_ 
--        ,denied_ignore_working_type
--        ,city_name
--        ,sum(total_order)*1.00/count(distinct shipper_id) as denied_ignore_per_drivers 



-- from 
-- (
    select   
        date_ 
        ,denied_ignore_working_type 
        ,final_status_working_type
        ,city_name
        -- ,shipper_id
        ,count(distinct case when rank = 1 then order_code else null end) as total_order
        -- ,count(distinct case when rank = 1 and issue_category = 'Ignore' then order_code else null end) as total_ignore
        -- ,count(distinct case when rank = 1 and issue_category != 'Ignore' then order_code else null end) as total_denied


from final 


-- and a.shipper_id = 22652660
where order_type = '1. Food/Market'

and issue_category = 'Ignore'
-- and order_id = 318911908

and denied_ignore_working_type = 'Hub Inshift'

group by 1,2,3,4

-- )

-- group by 1,2,3