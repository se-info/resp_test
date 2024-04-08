WITH params(period, start_date, end_date, days) AS (
    VALUES
    (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day), '%b'), DATE_TRUNC('month', current_date - interval '1' day), current_date - interval '1' day, CAST(DAY(current_date - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day), 'W%v'), DATE_TRUNC('week', current_date - interval '1' day), current_date - interval '1' day, CAST(DATE_DIFF('day', DATE_TRUNC('week', current_date - interval '1' day), current_date) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '7' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '7' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '1' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '14' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '14' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '8' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '21' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '21' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '15' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '28' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '28' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '22' day, CAST(7 AS DOUBLE))
    , (CAST(current_date - interval '1' day AS VARCHAR), current_date - interval '1' day, current_date - interval '1' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '2' day AS VARCHAR), current_date - interval '2' day, current_date - interval '2' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '3' day AS VARCHAR), current_date - interval '3' day, current_date - interval '3' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '4' day AS VARCHAR), current_date - interval '4' day, current_date - interval '4' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '5' day AS VARCHAR), current_date - interval '5' day, current_date - interval '5' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '6' day AS VARCHAR), current_date - interval '6' day, current_date - interval '6' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '7' day AS VARCHAR), current_date - interval '7' day, current_date - interval '7' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '8' day AS VARCHAR), current_date - interval '8' day, current_date - interval '8' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '9' day AS VARCHAR), current_date - interval '9' day, current_date - interval '9' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '10' day AS VARCHAR), current_date - interval '10' day, current_date - interval '10' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '11' day AS VARCHAR), current_date - interval '11' day, current_date - interval '11' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '12' day AS VARCHAR), current_date - interval '12' day, current_date - interval '12' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '13' day AS VARCHAR), current_date - interval '13' day, current_date - interval '13' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '14' day AS VARCHAR), current_date - interval '14' day, current_date - interval '14' day, CAST(1 AS DOUBLE))
    , (CAST(current_date - interval '15' day AS VARCHAR), current_date - interval '15' day, current_date - interval '15' day, CAST(1 AS DOUBLE))
    )
, grass_date AS (
SELECT
    grass_date
FROM
    ((SELECT sequence(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, current_date - interval '1' day) bar)
CROSS JOIN
    unnest (bar) as t(grass_date)
))
, driver_time AS (
SELECT d.*, IF(sm.shipper_type_id = 12, 'Hub', 'Non-hub') AS shipper_type
FROM
    (SELECT
        shipper_id
        , MIN(IF(total_online_time > 0, grass_date, NULL)) OVER (PARTITION BY shipper_id) first_date
        , total_online_time
        , total_working_time
        , grass_date
    FROM
        (SELECT
            shipper_id
            , create_date AS grass_date
            , CAST(DATE_DIFF('second', actual_start_time_online, actual_end_time_online) AS DOUBLE) / 3600 AS total_online_time
            , CAST(DATE_DIFF('second', actual_start_time_work, actual_end_time_work) AS DOUBLE) / 3600 AS total_working_time
        FROM
            (SELECT
                uid AS shipper_id
                ,DATE(from_unixtime(create_time - 3600)) AS create_date
                ,FROM_UNIXTIME(check_in_time - 3600) AS actual_start_time_online
                ,GREATEST(from_unixtime(check_out_time - 3600),from_unixtime(order_end_time - 3600)) AS actual_end_time_online
                ,IF(order_start_time = 0, FROM_UNIXTIME(check_in_time - 3600), FROM_UNIXTIME(order_start_time - 3600)) AS actual_start_time_work
                ,IF(order_end_time = 0, FROM_UNIXTIME(check_in_time - 3600), FROM_UNIXTIME(order_end_time - 3600)) AS actual_end_time_work
                FROM shopeefood.foody_internal_db__shipper_time_sheet_tab__reg_daily_s0_live
                WHERE 1=1
                AND check_in_time > 0
                AND check_out_time > 0
                AND check_out_time >= check_in_time
                AND order_end_time >= order_start_time
                AND ((order_start_time = 0 AND order_end_time = 0)
                    OR (order_start_time > 0 AND order_end_time > 0 AND order_start_time >= check_in_time AND order_start_time <= check_out_time)
                    )
            )
        )
    ) d
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON d.shipper_id = sm.shipper_id AND d.grass_date = TRY_CAST(sm.grass_date AS DATE)
WHERE d.grass_date BETWEEN DATE_TRUNC('month', current_date - interval '1' day) - interval '2' month AND current_date - interval '1' day
AND sm.grass_date != 'current'
)
, driver_order AS (
SELECT *
FROM
    (SELECT
        shipper_id
        , report_date
        , MIN(report_date) OVER (PARTITION BY shipper_id) AS first_date
        , order_uid
        , order_status
    FROM
        (SELECT dot.uid as shipper_id
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
              ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
              ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
            --   ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.city_name end as city_name
              ,case when dot.pick_city_id = 217 then 'HCM'
                    when dot.pick_city_id = 218 then 'HN'
                    when dot.pick_city_id = 219 then 'DN'
                    ELSE 'OTH' end as city_group
        FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
        LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
        )
    )
WHERE report_date BETWEEN DATE_TRUNC('month', current_date - interval '1' day) - interval '2' month AND current_date - interval '1' day
)
-- PART 1: Active drivers
, active_drivers AS (
SELECT
    p.period
    , p.days AS days
    , COUNT(DISTINCT (s.shipper_id, s.grass_date)) / p.days AS avg_active_drivers
    , COUNT(DISTINCT IF(sm.shipper_type_id = 12, (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_active_hub_drivers
    , COUNT(DISTINCT IF(sm.shipper_type_id != 12, (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_active_nonhub_drivers
    , COUNT(DISTINCT CASE WHEN s.grass_date = s.first_date THEN (s.shipper_id, s.grass_date) ELSE NULL END) / p.days AS avg_new_active_drivers
    , TRY(CAST(COUNT(DISTINCT CASE WHEN s.grass_date = first_date THEN (s.shipper_id, s.grass_date) ELSE NULL END) AS DOUBLE) / COUNT(DISTINCT (s.shipper_id, s.grass_date))) AS new_active_drivers_pct
FROM (
    SELECT
        shipper_id
        , grass_date
        , first_date
        , SUM(total_online_time) AS total_online_time
    FROM driver_time
    WHERE 1=1
    AND total_online_time > 0
    GROUP BY 1,2,3
        ) s
INNER JOIN params p ON s.grass_date BETWEEN p.start_date AND p.end_date
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON s.shipper_id = sm.shipper_id AND s.grass_date = TRY_CAST(sm.grass_date AS DATE)
WHERE sm.grass_date != 'current'
GROUP BY 1,2
    )
-- PART 2: Transacting drivers
, transacting_drivers AS (
SELECT
    p.period
    , p.days AS days
    , COUNT(DISTINCT (s.shipper_id, s.grass_date)) / p.days AS avg_transacting_drivers
    , COUNT(DISTINCT IF(sm.shipper_type_id = 12 AND sm.city_name IN ('HCM City', 'Ha Noi City'), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_hub_hcm_hn
    , COUNT(DISTINCT IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (1,6,11), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_t1_hcm_hn
    , COUNT(DISTINCT IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (2,7,12), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_t2_hcm_hn
    , COUNT(DISTINCT IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (3,8,13), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_t3_hcm_hn
    , COUNT(DISTINCT IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (4,9,14), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_t4_hcm_hn
    , COUNT(DISTINCT IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (5,10,15), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_t5_hcm_hn
    , COUNT(DISTINCT IF(sm.shipper_type_id != 12 AND sm.city_name NOT IN ('HCM City', 'Ha Noi City'), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_dn_oth
    , COUNT(DISTINCT CASE WHEN s.grass_date = s.first_date THEN (s.shipper_id, s.grass_date) ELSE NULL END) / p.days AS avg_new_transacting_drivers
    , TRY(CAST(COUNT(DISTINCT CASE WHEN s.grass_date = s.first_date THEN (s.shipper_id, s.grass_date) ELSE NULL END) AS DOUBLE) / COUNT(DISTINCT (s.shipper_id, s.grass_date))) AS new_transacting_drivers_pct
    , SUM(s.delivered_orders) / p.days AS _delivered_ado
    , SUM(IF(sm.shipper_type_id = 12 AND sm.city_name IN ('HCM City', 'Ha Noi City'), s.delivered_orders, NULL)) / p.days AS _delivered_ado_hub_hcm_hn
    , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (1,6,11), s.delivered_orders, 0)) / p.days AS _delivered_ado_t1_hcm_hn
    , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (2,7,12), s.delivered_orders, 0)) / p.days AS _delivered_ado_t2_hcm_hn
    , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (3,8,13), s.delivered_orders, 0)) / p.days AS _delivered_ado_t3_hcm_hn
    , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (4,9,14), s.delivered_orders, 0)) / p.days AS _delivered_ado_t4_hcm_hn
    , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (5,10,15), s.delivered_orders, 0)) / p.days AS _delivered_ado_t5_hcm_hn
    , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name NOT IN ('HCM City', 'Ha Noi City'), s.delivered_orders, 0)) / p.days AS _delivered_ado_dn_oth
FROM (
    SELECT
        shipper_id
        , report_date AS grass_date
        , first_date
        , COUNT(DISTINCT order_uid) AS delivered_orders
    FROM driver_order
    WHERE order_status = 400
    GROUP BY 1,2,3
        ) s
INNER JOIN params p ON s.grass_date BETWEEN p.start_date AND p.end_date
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON s.shipper_id = sm.shipper_id AND s.grass_date = TRY_CAST(sm.grass_date AS DATE)
LEFT JOIN shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus ON s.shipper_id = bonus.uid AND s.grass_date = DATE(from_unixtime(bonus.report_date - 3600))
WHERE sm.grass_date != 'current'
GROUP BY 1,2
    )
-- PART 3: Supply hours
, supply_hours AS (
SELECT
    p.period
    , p.days AS days
    , SUM(sp.total_online_time) / p.days AS supply_time_active_drivers
    , SUM(IF(sp.shipper_type = 'Hub', sp.total_online_time, 0)) / p.days AS supply_time_active_hub_drivers
    , SUM(IF(sp.shipper_type = 'Non-hub', sp.total_online_time, 0)) / p.days AS supply_time_active_nonhub_drivers
    , SUM(IF(d.shipper_id IS NOT NULL, sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id = 12 AND sm.city_name IN ('HCM City', 'Ha Noi City'), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_hub_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (1,6,11), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_t1_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (2,7,12), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_t2_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (3,8,13), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_t3_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (4,9,14), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_t4_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (5,10,15), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_t5_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name NOT IN ('HCM City', 'Ha Noi City'), sp.total_online_time, 0)) / p.days AS supply_time_transacting_drivers_dn_oth
    , SUM(IF(d.shipper_id IS NOT NULL, sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id = 12 AND sm.city_name IN ('HCM City', 'Ha Noi City'), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_hub_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (1,6,11), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_t1_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (2,7,12), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_t2_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (3,8,13), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_t3_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (4,9,14), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_t4_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (5,10,15), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_t5_hcm_hn
    , SUM(IF(d.shipper_id IS NOT NULL AND sm.shipper_type_id != 12 AND sm.city_name NOT IN ('HCM City', 'Ha Noi City'), sp.total_working_time, 0)) / p.days AS _onjob_time_transacting_drivers_dn_oth
FROM driver_time sp
LEFT JOIN ( --transacting drivers
    SELECT
        shipper_id
        , report_date AS grass_date
    FROM driver_order
    WHERE order_status = 400
    AND report_date BETWEEN DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month AND current_date - interval '1' day
    GROUP BY 1,2
        ) d ON sp.shipper_id = d.shipper_id AND sp.grass_date = d.grass_date
INNER JOIN params p ON sp.grass_date BETWEEN p.start_date AND p.end_date
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON sp.shipper_id = sm.shipper_id AND sp.grass_date = TRY_CAST(sm.grass_date AS DATE)
LEFT JOIN shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus ON sp.shipper_id = bonus.uid AND sp.grass_date = DATE(from_unixtime(bonus.report_date - 3600))
WHERE sm.grass_date != 'current'
GROUP BY 1,2
    )
-- PART 4: Churn/Reactivate
, status AS (
SELECT
    period
    , days
    , COUNT(DISTINCT(shipper_status_code = 1, (shipper_id, grass_date), NULL)) / days AS _avg_platform_active_drivers
    , COUNT(DISTINCT IF(COALESCE(l14d_ago_delivered_orders, 0) > 0 AND COALESCE(l14d_delivered_orders, 0) = 0 AND COALESCE(l0d_delivered_orders, 0) = 0, (shipper_id, grass_date), NULL)) / days AS avg_churned_drivers
    , COUNT(DISTINCT IF(COALESCE(l14d_ago_delivered_orders, 0) > 0 AND COALESCE(l14d_delivered_orders, 0) = 0 AND COALESCE(l0d_delivered_orders, 0) > 0, (shipper_id, grass_date), NULL)) / days AS avg_reactivated_drivers
FROM
    (SELECT
        p.period
        , p.days
        , TRY_CAST(sm.grass_date AS DATE) AS grass_date
        , sm.shipper_id
        , sm.shipper_status_code
        , SUM(l14d_ago.delivered_orders) AS l14d_ago_delivered_orders
        , SUM(l14d.delivered_orders) AS l14d_delivered_orders
        , SUM(l0d.delivered_orders) AS l0d_delivered_orders
    FROM shopeefood.foody_mart__profile_shipper_master sm
    INNER JOIN params p ON TRY_CAST(sm.grass_date AS DATE) BETWEEN p.start_date AND p.end_date
    LEFT JOIN (
        SELECT
            shipper_id
            , report_date AS grass_date
            , COUNT(DISTINCT order_uid) AS delivered_orders
            FROM driver_order
            WHERE order_status = 400
            GROUP BY 1,2
        ) l14d ON sm.shipper_id = l14d.shipper_id AND l14d.grass_date BETWEEN TRY_CAST(sm.grass_date AS DATE) - interval '14' day  AND TRY_CAST(sm.grass_date AS DATE) - interval '1' day

    LEFT JOIN (
        SELECT
            shipper_id
            , report_date AS grass_date
            , COUNT(DISTINCT order_uid) AS delivered_orders
            FROM driver_order
            WHERE order_status = 400
            GROUP BY 1,2
        ) l14d_ago ON sm.shipper_id = l14d_ago.shipper_id AND l14d_ago.grass_date < TRY_CAST(sm.grass_date AS DATE) - interval '14' day
    LEFT JOIN (
        SELECT
            shipper_id
            , report_date AS grass_date
            , COUNT(DISTINCT order_uid) AS delivered_orders
            FROM driver_order
            WHERE order_status = 400
            GROUP BY 1,2
        ) l0d ON sm.shipper_id = l0d.shipper_id AND TRY_CAST(sm.grass_date AS DATE) = l0d.grass_date
    WHERE sm.grass_date != 'current'
    GROUP BY 1,2,3,4,5
        )
GROUP BY 1,2
    )

-- FINALE
SELECT
    -- Part 2: Transacting drivers
    t.period
    , t.days AS days
    , t.avg_transacting_drivers
    , t.avg_transacting_drivers_hub_hcm_hn
    , t.avg_transacting_drivers_t1_hcm_hn
    , t.avg_transacting_drivers_t2_hcm_hn
    , t.avg_transacting_drivers_t3_hcm_hn
    , t.avg_transacting_drivers_t4_hcm_hn
    , t.avg_transacting_drivers_t5_hcm_hn
    , t.avg_transacting_drivers_dn_oth
    , t.avg_new_transacting_drivers
    , t.new_transacting_drivers_pct
    , t._delivered_ado
    , t._delivered_ado_hub_hcm_hn
    , t._delivered_ado_t1_hcm_hn
    , t._delivered_ado_t2_hcm_hn
    , t._delivered_ado_t3_hcm_hn
    , t._delivered_ado_t4_hcm_hn
    , t._delivered_ado_t5_hcm_hn
    , t._delivered_ado_dn_oth
    -- Part 1: Active drivers
    , a.avg_active_drivers
    , a.avg_new_active_drivers
    , a.new_active_drivers_pct
    , a.avg_active_hub_drivers
    , a.avg_active_nonhub_drivers
    -- Part 3: Supply hours
    , s.supply_time_active_drivers
--     , TRY(s.supply_time_active_hub_drivers / avg_active_hub_drivers) AS supply_time_active_hub_drivers
--     , TRY(s.supply_time_active_nonhub_drivers / avg_active_nonhub_drivers) AS supply_time_active_nonhub_drivers
    , s.supply_time_transacting_drivers
    , s.supply_time_transacting_drivers_hub_hcm_hn
    , s.supply_time_transacting_drivers_t1_hcm_hn
    , s.supply_time_transacting_drivers_t2_hcm_hn
    , s.supply_time_transacting_drivers_t3_hcm_hn
    , s.supply_time_transacting_drivers_t4_hcm_hn
    , s.supply_time_transacting_drivers_t5_hcm_hn
    , s.supply_time_transacting_drivers_dn_oth
    , s._onjob_time_transacting_drivers
    , s._onjob_time_transacting_drivers_hub_hcm_hn
    , s._onjob_time_transacting_drivers_t1_hcm_hn
    , s._onjob_time_transacting_drivers_t2_hcm_hn
    , s._onjob_time_transacting_drivers_t3_hcm_hn
    , s._onjob_time_transacting_drivers_t4_hcm_hn
    , s._onjob_time_transacting_drivers_t5_hcm_hn
    , s._onjob_time_transacting_drivers_dn_oth
    -- Part 4: Status
    , stt._avg_platform_active_drivers
    , stt.avg_churned_drivers
    , stt.avg_reactivated_drivers
FROM transacting_drivers t
LEFT JOIN active_drivers a ON t.period = a.period
LEFT JOIN supply_hours s ON t.period = s.period
LEFT JOIN status stt ON t.period = stt.period