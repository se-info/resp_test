with raw1 as 
(select 
 a.id 
,a.uid 
,date(from_unixtime(a.create_time - 3600)) as created_date
,from_unixtime(a.check_in_time - 3600)  as check_in_time 
,from_unixtime(a.check_out_time - 3600) as check_out_time
,date_diff('second',from_unixtime(a.check_in_time - 3600),from_unixtime(a.check_out_time - 3600))*1.00/60 as diff_checkin_checkout
,row_number()over(partition by uid,date(from_unixtime(a.create_time - 3600)) order by create_time asc) as rank 

from shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live a

-- where date(from_unixtime(a.create_time - 3600)) between date'2022-06-06' and date'2022-06-07'
-- and a.uid = 16762663


 )

,base as 
(select 
 a.id 
,a.uid as shipper_id 
,sm.shipper_name
,sm.city_name
,a.created_date
,a.check_in_time
,a.check_out_time
-- ,date_diff('second',from_unixtime(a.check_in_time - 3600),from_unixtime(a.check_out_time - 3600))*1.00/60 as diff_checkin_checkout
,date_diff('second',b.check_out_time,a.check_in_time)*1.00/60 as diff_checkin_checkout
,current.current_driver_tier as tier_ 
,service_level_rate


from raw1 a 


left join raw1 b on b.uid = a.uid 
                 and b.rank = a.rank - 1 
                 and a.created_date = b.created_date



--Driver Info 
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = a.uid and try_cast(sm.grass_date as date) = a.created_date


--Driver assign performance 



-- Driver Tier 
LEFT JOIN

        (SELECT cast(from_unixtime(bonus.report_date - 60*60) as date) as report_date
        ,bonus.uid as shipper_id
        ,case when hub.shipper_type_id = 12 then 'Hub'
        when bonus.tier in (1,6,11) then 'T1' when bonus.tier in (2,7,12) then 'T2'
        when bonus.tier in (3,8,13) then 'T3'
        when bonus.tier in (4,9,14) then 'T4'
        when bonus.tier in (5,10,15) then 'T5'
        else null end as current_driver_tier
        ,bonus.total_point
        ,bonus.daily_point
        ,completed_rate*1.00/100 as service_level_rate

        FROM shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus

LEFT JOIN

        (SELECT shipper_id
        ,shipper_type_id
        ,case when grass_date = 'current' then date(current_date)
        else cast(grass_date as date) end as report_date

        from shopeefood.foody_mart__profile_shipper_master

        where 1=1
        and (grass_date = 'current' OR cast(grass_date as date) >= date('2019-01-01'))
        GROUP BY 1,2,3
        )hub on hub.shipper_id = bonus.uid and hub.report_date = cast(from_unixtime(bonus.report_date - 60*60) as date)

        where cast(from_unixtime(bonus.report_date - 60*60) as date) between current_date - interval '30' day and current_date - interval '1' day

        )current on current.shipper_id = a.uid and current.report_date =  a.created_date
where 1 = 1
and a.created_date between current_date - interval '14' day and current_date - interval '1' day

)

,assignment AS
(SELECT
    COALESCE(assign.date_, deny.deny_date) AS report_date
    , COALESCE(assign.shipper_id, deny.shipper_id) AS shipper_id
    -- , COALESCE(assign.time_range, deny.time_range) AS time_range
    , COALESCE(assign.cnt_total_assign_order,0) + COALESCE(deny.cnt_deny_acceptable,0) AS cnt_total_assign_order
    , COALESCE(assign.cnt_total_incharge,0) AS cnt_total_incharge
    , COALESCE(assign.cnt_ignore_total,0) AS cnt_ignore_total
    , COALESCE(deny.cnt_deny_acceptable,0) AS cnt_deny_total
    FROM
        (SELECT
            date_
            , shipper_id
            -- , CASE
            --     WHEN create_hour * 100 + create_minute >= 1030 AND create_hour * 100 + create_minute <= 1230 THEN '10h30 - 12h30'
            --     WHEN create_hour * 100 + create_minute >= 1730 AND create_hour * 100 + create_minute <= 1900 THEN '17h30 - 19h'
            -- END AS time_range
            , COUNT(DISTINCT order_uid) AS cnt_total_assign_order
            , COUNT(DISTINCT IF(status IN (3,4), order_uid, NULL)) AS cnt_total_incharge
            , COUNT(DISTINCT IF(status IN (8,9,17,18), order_uid, NULL)) AS cnt_ignore_total

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
            )
        
        GROUP BY 1,2
        ) assign

    FULL JOIN

        (SELECT
            deny_date
            , shipper_id
            -- , CASE
            --     WHEN deny_hour * 100 + deny_minute >= 1030 AND deny_hour * 100 + deny_minute <= 1230 THEN '10h30 - 12h30'
            --     WHEN deny_hour * 100 + deny_minute >= 1730 AND deny_hour * 100 + deny_minute <= 1900 THEN '17h30 - 19h'
            -- END AS time_range
            , COUNT(ref_order_code) AS cnt_deny_total
            , COUNT(ref_order_code) AS cnt_deny_acceptable
            , COUNT(IF(deny_type = 'Driver_Fault', ref_order_code, NULL)) AS cnt_deny_non_acceptable
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
            WHERE DATE(FROM_UNIXTIME(dod.create_time - 3600)) BETWEEN current_date - interval '30' day and current_date - interval '1' day
            ) dod
        group by 1,2
        ) deny on assign.date_ = deny.deny_date AND assign.shipper_id = deny.shipper_id 
)

select 
       a.created_date
      ,a.shipper_id
      ,a.shipper_name
      ,a.city_name
      ,a.tier_ 
      ,a.service_level_rate
      ,b.cnt_total_assign_order
      ,b.cnt_total_incharge
      ,b.cnt_ignore_total
      ,b.cnt_deny_total
      ,count(case when diff_checkin_checkout <= 3 then id else null end) as total_time_checkin_checkout_3min
      ,count(case when diff_checkin_checkout <= 1 then id else null end) as total_time_checkin_checkout_1min






from base a 

left join assignment b on b.shipper_id = a.shipper_id and a.created_date = b.report_date 

where 1 = 1  
and a.created_date >= ${start_date}
and a.created_date <= ${end_date}


group by 1,2,3,4,5,6,7,8,9,10

having (count(case when diff_checkin_checkout <= 3 then id else null end) > 0 
        or 
        count(case when diff_checkin_checkout <= 1 then id else null end) > 0)




        