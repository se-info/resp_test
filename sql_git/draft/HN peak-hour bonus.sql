WITH assignment AS
(SELECT
    COALESCE(assign.date_, deny.deny_date) AS report_date
    , COALESCE(assign.shipper_id, deny.shipper_id) AS shipper_id
    , COALESCE(assign.time_range, deny.time_range) AS time_range
    , COALESCE(assign.cnt_total_assign_order,0) + COALESCE(deny.cnt_deny_acceptable,0) AS cnt_total_assign_order
    , COALESCE(assign.cnt_total_incharge,0) AS cnt_total_incharge
    , COALESCE(assign.cnt_ignore_total,0) AS cnt_ignore_total
    , COALESCE(deny.cnt_deny_acceptable,0) AS cnt_deny_total
    FROM
        (SELECT
            date_
            , shipper_id
            , CASE
                WHEN create_hour * 100 + create_minute >= 1030 AND create_hour * 100 + create_minute <= 1230 THEN '10h30 - 12h30'
                WHEN create_hour * 100 + create_minute >= 1730 AND create_hour * 100 + create_minute <= 1900 THEN '17h30 - 19h'
            END AS time_range
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
                AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2022-01-29' AND DATE'2022-02-06'

                UNION ALL

                SELECT
                    CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                    , order_id, city_id, assign_type, update_time, create_time, status, order_type
                    , experiment_group, shipper_uid AS shipper_id

                FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                WHERE status IN (3,4,8,9) -- shipper incharge + ignore
                AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2022-02-08' AND DATE'2022-02-10'
                ) a
            )
        WHERE (create_hour * 100 + create_minute >= 1030 AND create_hour * 100 + create_minute <= 1230)
        OR (create_hour * 100 + create_minute >= 1730 AND create_hour * 100 + create_minute <= 1900)
        GROUP BY 1,2,3
        ) assign

    FULL JOIN

        (SELECT
            deny_date
            , shipper_id
            , CASE
                WHEN deny_hour * 100 + deny_minute >= 1030 AND deny_hour * 100 + deny_minute <= 1230 THEN '10h30 - 12h30'
                WHEN deny_hour * 100 + deny_minute >= 1730 AND deny_hour * 100 + deny_minute <= 1900 THEN '17h30 - 19h'
            END AS time_range
            , COUNT(ref_order_code) AS cnt_deny_total
            , COUNT(IF(deny_type = 'Driver_Fault', ref_order_code, NULL)) AS cnt_deny_acceptable
            , COUNT(IF(deny_type <> 'Driver_Fault', ref_order_code, NULL)) AS cnt_deny_non_acceptable
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
            WHERE DATE(FROM_UNIXTIME(dod.create_time - 3600)) BETWEEN DATE'2022-02-08' AND DATE'2022-02-10'
            ) dod
        group by 1,2,3
        ) deny on assign.date_ = deny.deny_date AND assign.shipper_id = deny.shipper_id AND assign.time_range = deny.time_range
)
, snp AS
(SELECT
    shipper_id
    , delivered_date
    , CASE
        WHEN delivered_hour * 100 + delivered_minute >= 1030 AND delivered_hour * 100 + delivered_minute <= 1230 THEN '10h30 - 12h30'
        WHEN delivered_hour * 100 + delivered_minute >= 1730 AND delivered_hour * 100 + delivered_minute <= 1900 THEN '17h30 - 19h'
    END AS time_range
    , COUNT(DISTINCT order_uid) AS cnt_total_order_delivered
FROM
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

        -- ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(FROM_UNIXTIME(dot.real_drop_time - 60*60))
        --     when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(FROM_UNIXTIME(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
        --     else date(FROM_UNIXTIME(dot.submitted_time- 60*60)) end as report_date
        ,date(FROM_UNIXTIME(dot.submitted_time- 60*60)) created_date
        ,date(case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 60*60) end) as delivered_date
        ,case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 60*60) end as last_delivered_timestamp
        ,HOUR(case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 60*60) end) AS delivered_hour
        ,MINUTE(case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 60*60) end) AS delivered_minute
        --   ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
        ,case when dot.pick_city_id = 217 then 'HCM'
            when dot.pick_city_id = 218 then 'HN'
            when dot.pick_city_id = 219 then 'DN'
            ELSE 'OTH' end as city_group
    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
    WHERE dot.order_status = 400
    AND dot.pick_city_id = 218
    AND dot.ref_order_category = 0
    AND DATE(case when dot.real_drop_time = 0 then null else FROM_UNIXTIME(dot.real_drop_time - 60*60) end) BETWEEN DATE'2022-02-08' AND DATE'2022-02-10'
    )
GROUP BY 1,2,3
)
SELECT
    *
FROM
    (SELECT
        snp.shipper_id
        , sm.shipper_name
        , sm.city_name
        , snp.delivered_date
        , snp.time_range
        , CAST(COALESCE(a.cnt_total_incharge, 0) AS DOUBLE) / a.cnt_total_assign_order AS sla
        , snp.cnt_total_order_delivered
    FROM snp
    LEFT JOIN assignment a ON snp.delivered_date = a.report_date AND snp.shipper_id = a.shipper_id AND snp.time_range = a.time_range
    LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON snp.shipper_id = sm.shipper_id AND TRY_CAST(sm.grass_date AS DATE) = snp.delivered_date
    WHERE ((snp.delivered_date = DATE'2022-02-08' AND snp.time_range = '17h30 - 19h')
    OR (snp.delivered_date = DATE'2022-02-09' AND snp.time_range IN ('10h30 - 12h30', '17h30 - 19h'))
    OR (snp.delivered_date = DATE'2022-02-10' AND snp.time_range IN ('10h30 - 12h30', '17h30 - 19h')))
    AND sm.shipper_type_id != 12
    )
WHERE sla >= 0.99
ORDER BY 1,3,4