WITH params (period, start_date, end_date, days) AS (
    VALUES

    -- (DATE_FORMAT(DATE_TRUNC('month', date '2021-12-01'), '%b'), date '2021-12-01', date '2021-12-31', 31.0),
    -- (DATE_FORMAT(DATE_TRUNC('month', date '2022-01-01'), '%b'), date '2022-01-01', date '2022-01-31', 31.0),
    -- (DATE_FORMAT(DATE_TRUNC('month', date '2022-02-01'), '%b'), date '2022-02-01', date '2022-02-28', 28.0),
    (DATE_FORMAT(DATE_TRUNC('month', date '2022-07-01'), '%b'), date '2022-07-01', date '2022-07-31', 31.0)
    
)
, grass_date AS (
SELECT
    grass_date
FROM
    ((SELECT sequence(date '2022-07-01', date '2022-07-31') bar)
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
        , driver_payment_policy
        , source
    FROM
        (SELECT dot.uid as shipper_id
              ,dot.ref_order_id as order_id
              ,dot.ref_order_code as order_code
              ,CAST(dot.ref_order_id AS VARCHAR) || '-' || CAST(dot.ref_order_category AS VARCHAR) AS order_uid
              ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
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
WHERE report_date BETWEEN current_date - interval '60' day and current_date - interval '1' day
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
, driver_type as 
(SELECT
                shipper_id
                ,shipper_type_id
                ,report_date
                ,shipper_shift_id
                ,start_shift
                ,end_shift
                ,off_weekdays
                ,registration_status
                ,if(report_date < DATE'2021-10-22' and off_date is null and registration_status is null, off_date_1, off_date) as off_date
            FROM
                (
                SELECT  sm.shipper_id
                        ,sm.shipper_type_id
                        ,try_cast(sm.grass_date as date) as report_date
                        ,sm.shipper_shift_id
                        ,case
                            when try_cast(sm.grass_date as date) < date'2021-10-22' then
                                case
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                          ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                          ) then if((ss2.end_time - ss2.start_time)*1.00/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.00/3600 < 10.00, (ss2.end_time - 28800)/3600, ss2.start_time/3600)
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                          ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                          ) then null
                                    else if((ss1.end_time - ss1.start_time)*1.00/3600 > 5.00 and (ss1.end_time - ss1.start_time)*1.00/3600 < 10.00, (ss1.end_time - 28800)/3600, ss1.start_time/3600)
                                end
                            else
                                if(ss2.end_time is not null, if((ss2.end_time - ss2.start_time)*1.00/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.00/3600 < 10.00, (ss2.end_time - 28800)/3600, ss2.start_time/3600)
                                    ,if((ss1.end_time - ss1.start_time)*1.00/3600 > 5.00 and (ss1.end_time - ss1.start_time)*1.00/3600 < 10.00, (ss1.end_time - 28800)/3600, ss1.start_time/3600)
                                )
                        end as start_shift
                        ,case
                            when try_cast(sm.grass_date as date) < date'2021-10-22' then
                                case
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                          ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                          ) then ss2.end_time/3600
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                          ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                          ) then null
                                    else ss1.end_time/3600
                                end
                            else
                                if(ss2.end_time is not null, ss2.end_time/3600, ss1.end_time/3600)
                        end as end_shift
                        ,case
                            when ss2.registration_status = 1 then 'Registered'
                            when ss2.registration_status = 2 then 'Off'
                            when ss2.registration_status = 3 then 'Work'
                        else
                            case
                                when try_cast(sm.grass_date as date) < date'2021-10-22' then null
                            else 'Off' end
                        end as registration_status
                        ,case
                            when try_cast(sm.grass_date as date) < date'2021-10-22' then
                                case
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                          ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                          ) then if(ss2.registration_status = 2, case
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                                end, null)
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                          ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                          ) then case
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                                end
                                    else ss1.off_weekdays
                                end
                            else
                                if(ss2.end_time is not null, if(ss2.registration_status = 2, case
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                                end, null), case
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                                    when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                                end)
                        end as off_weekdays
                        ,case
                            when try_cast(sm.grass_date as date) < date'2021-10-22' then
                                case
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is not null)
                                            or
                                          (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                          ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is not null)
                                          ) then if(ss2.registration_status = 2, date_format(try_cast(sm.grass_date as date), '%a'), null)
                                    when ((sm.shipper_id in (9887416,4826244) and try_cast(sm.grass_date as date) >= date'2021-10-08' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (16814977) and try_cast(sm.grass_date as date) >= date'2021-10-09' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (4851147) and try_cast(sm.grass_date as date) >= date'2021-10-10' and ss2.end_time is null)
                                            or
                                          (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                          ) and try_cast(sm.grass_date as date) >= date'2021-10-14' and ss2.end_time is null)
                                          ) then date_format(try_cast(sm.grass_date as date), '%a')
                                    else null
                                end
                            else
                                if(ss2.end_time is not null, if(ss2.registration_status = 2, date_format(try_cast(sm.grass_date as date), '%a'), null)
                                , date_format(try_cast(sm.grass_date as date), '%a'))
                        end as off_date
                        ,array_join(array_agg(cast(d_.cha_date as VARCHAR)),', ') as off_date_1

                        from shopeefood.foody_mart__profile_shipper_master sm
                        left join shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss1 on ss1.id = sm.shipper_shift_id
                        left join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live ss2 on ss2.uid = sm.shipper_id and date(from_unixtime(ss2.date_ts-3600)) = try_cast(sm.grass_date as date)
                        left join
                                 (SELECT
                                         case when off_weekdays = '1' then 'Mon'
                                              when off_weekdays = '2' then 'Tue'
                                              when off_weekdays = '3' then 'Wed'
                                              when off_weekdays = '4' then 'Thu'
                                              when off_weekdays = '5' then 'Fri'
                                              when off_weekdays = '6' then 'Sat'
                                              when off_weekdays = '7' then 'Sun'
                                              else 'No off date' end as cha_date
                                         ,off_weekdays as num_date

                                  FROM shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live
                                  WHERE 1=1
                                  and off_weekdays in ('1','2','3','4','5','6','7')
                                  GROUP BY 1,2
                                 )d_ on regexp_like(ss1.off_weekdays,cast(d_.num_date  as varchar)) = true

                        where 1=1
                        and sm.grass_region = 'VN'
                        and try_cast(sm.grass_date as date) between DATE_TRUNC('month', current_date - interval '1' day) - interval '2' month and current_date - interval '1' day
                        GROUP BY 1,2,3,4,5,6,7,8,9
                )
)
, transacting_drivers AS (
SELECT
    p.period
    , p.days AS days
    -- , COUNT(DISTINCT (s.shipper_id, s.grass_date)) / p.days AS avg_transacting_drivers
    -- , COUNT(DISTINCT IF(sm.shipper_type_id = 12, (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_hub_drivers
    -- , COUNT(DISTINCT IF(sm.shipper_type_id != 12, (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_nonhub_drivers
    -- , COUNT(DISTINCT IF(sm.shipper_type_id = 12 AND sm.city_name IN ('HCM City', 'Ha Noi City'), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_hub_hcm_hn
    -- , COUNT(DISTINCT IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (1,6,11), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_t1_hcm_hn
    -- , COUNT(DISTINCT IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (2,7,12), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_t2_hcm_hn
    -- , COUNT(DISTINCT IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (3,8,13), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_t3_hcm_hn
    -- , COUNT(DISTINCT IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (4,9,14), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_t4_hcm_hn
    -- , COUNT(DISTINCT IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (5,10,15), (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_t5_hcm_hn
    -- , COUNT(DISTINCT IF((sm.shipper_type_id != 12 AND sm.city_name NOT IN ('HCM City', 'Ha Noi City')) or (sm.shipper_type_id = 1) , (s.shipper_id, s.grass_date), NULL)) / p.days AS avg_transacting_drivers_dn_oth
    -- , COUNT(DISTINCT CASE WHEN s.grass_date = s.first_date THEN (s.shipper_id, s.grass_date) ELSE NULL END) / p.days AS avg_new_transacting_drivers
    -- , TRY(CAST(COUNT(DISTINCT CASE WHEN s.grass_date = s.first_date THEN (s.shipper_id, s.grass_date) ELSE NULL END) AS DOUBLE) / COUNT(DISTINCT (s.shipper_id, s.grass_date))) AS new_transacting_drivers_pct
    , COUNT(DISTINCT IF(sm.shipper_type_id = 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND driver_type.end_shift - driver_type.start_shift = 10, (s.shipper_id, s.grass_date), NULL)) / p.days AS _active_hub_10
    , COUNT(DISTINCT IF(sm.shipper_type_id = 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND driver_type.end_shift - driver_type.start_shift = 8, (s.shipper_id, s.grass_date), NULL)) / p.days AS _active_hub_8
    , COUNT(DISTINCT IF(sm.shipper_type_id = 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND driver_type.end_shift - driver_type.start_shift = 5, (s.shipper_id, s.grass_date), NULL)) / p.days AS _active_hub_5    
    , COUNT(DISTINCT IF(sm.shipper_type_id = 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND driver_type.end_shift - driver_type.start_shift in (3,4), (s.shipper_id, s.grass_date), NULL)) / p.days AS _active_hub_3    
    , SUM(s.delivered_orders) / p.days AS _delivered_ado
    , SUM(IF(sm.shipper_type_id = 12 , s.delivered_orders, NULL)) / p.days AS _delivered_ado_hub_hcm_hn
    , SUM(IF(sm.shipper_type_id = 12 AND driver_type.end_shift - driver_type.start_shift = 10 , s.delivered_orders, NULL)) / p.days AS _delivered_ado_hub_10
    , SUM(IF(sm.shipper_type_id = 12 AND driver_type.end_shift - driver_type.start_shift = 8 , s.delivered_orders, NULL)) / p.days AS _delivered_ado_hub_8
    , SUM(IF(sm.shipper_type_id = 12 AND driver_type.end_shift - driver_type.start_shift = 5 , s.delivered_orders, NULL)) / p.days AS _delivered_ado_hub_5
    , SUM(IF(sm.shipper_type_id = 12 AND driver_type.end_shift - driver_type.start_shift in (3,4) , s.delivered_orders, NULL)) / p.days AS _delivered_ado_hub_3
    -- , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (1,6,11), s.delivered_orders, 0)) / p.days AS _delivered_ado_t1_hcm_hn
    -- , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (2,7,12), s.delivered_orders, 0)) / p.days AS _delivered_ado_t2_hcm_hn
    -- , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (3,8,13), s.delivered_orders, 0)) / p.days AS _delivered_ado_t3_hcm_hn
    -- , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (4,9,14), s.delivered_orders, 0)) / p.days AS _delivered_ado_t4_hcm_hn
    -- , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name IN ('HCM City', 'Ha Noi City') AND bonus.tier in (5,10,15), s.delivered_orders, 0)) / p.days AS _delivered_ado_t5_hcm_hn
    -- , SUM(IF(sm.shipper_type_id != 12 AND sm.city_name NOT IN ('HCM City', 'Ha Noi City'), s.delivered_orders, 0)) / p.days AS _delivered_ado_dn_oth
FROM (
    SELECT
        shipper_id
        , report_date AS grass_date
        , first_date
        , COUNT(DISTINCT order_uid) AS delivered_orders
    FROM driver_order
    WHERE order_status = 400
    and  source = 'order_delivery'
    and driver_payment_policy = 2 
    GROUP BY 1,2,3
        ) s
INNER JOIN params p ON s.grass_date BETWEEN p.start_date AND p.end_date
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON s.shipper_id = sm.shipper_id AND s.grass_date = TRY_CAST(sm.grass_date AS DATE)
LEFT JOIN shopeefood.foody_internal_db__shipper_daily_bonus_log_tab__reg_daily_s0_live bonus ON s.shipper_id = bonus.uid AND s.grass_date = DATE(from_unixtime(bonus.report_date - 3600))
left join driver_type
	on driver_type.shipper_id = s.shipper_id and driver_type.report_date = s.grass_date
WHERE sm.grass_date != 'current'
and sm.city_name != 'Dien Bien'
and sm.shipper_type_id = 12 
GROUP BY 1,2
    )

-- FINALE
SELECT
    -- Part 2: Transacting drivers
    -- t.period
    -- , t.days AS days
    -- , t.avg_transacting_drivers
    -- , t.avg_transacting_hub_drivers
    -- , t.avg_transacting_nonhub_drivers
    -- , t.avg_transacting_drivers_hub_hcm_hn
    -- , t.avg_transacting_drivers_t1_hcm_hn
    -- , t.avg_transacting_drivers_t2_hcm_hn
    -- , t.avg_transacting_drivers_t3_hcm_hn
    -- , t.avg_transacting_drivers_t4_hcm_hn
    -- , t.avg_transacting_drivers_t5_hcm_hn
    -- , t.avg_transacting_drivers_dn_oth
    -- , t.avg_new_transacting_drivers
    -- , t.new_transacting_drivers_pct
    -- , t._delivered_ado
    -- , t._delivered_ado_hub_hcm_hn
    -- , t._delivered_ado_t1_hcm_hn
    -- , t._delivered_ado_t2_hcm_hn
    -- , t._delivered_ado_t3_hcm_hn
    -- , t._delivered_ado_t4_hcm_hn
    -- , t._delivered_ado_t5_hcm_hn
    -- , t._delivered_ado_dn_oth
    -- -- Part 1: Active drivers
    -- , a.avg_active_drivers
    -- , a.avg_new_active_drivers
    -- , a.new_active_drivers_pct
    -- , a.avg_active_hub_drivers
    -- , a.avg_active_nonhub_drivers
    t.*
FROM transacting_drivers t
LEFT JOIN active_drivers a ON t.period = a.period


-- create table shipper_phone_temp as
-- select *
-- from shopeefood.foody_internal_db__shipper_info_contact_tab__reg_daily_s2_live

-- select 
--     *
-- from shipper_phone_temp



