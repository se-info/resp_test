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
, shift AS
(WITH base AS
    (SELECT
        shipper_id
        ,shipper_name
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
        SELECT  coalesce(sm.shipper_id,ss2.uid) as shipper_id
                ,sm.shipper_name
                ,sm.shipper_type_id
                ,coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) as report_date
                ,sm.shipper_shift_id
                ,case
                    when coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) < date'2021-10-22' then
                        case
                            when ((sm.shipper_id in (9887416,4826244) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-08' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (16814977) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-09' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (4851147) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-10' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-14' and ss2.end_time is not null)
                                  ) then if((ss2.end_time - ss2.start_time)*1.00/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.00/3600 < 10.00, (ss2.end_time - 28800)/3600, ss2.start_time/3600)
                            when ((sm.shipper_id in (9887416,4826244) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-08' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (16814977) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-09' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (4851147) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-10' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-14' and ss2.end_time is null)
                                  ) then null
                            else if((ss1.end_time - ss1.start_time)*1.00/3600 > 5.00 and (ss1.end_time - ss1.start_time)*1.00/3600 < 10.00, (ss1.end_time - 28800)/3600, ss1.start_time/3600)
                        end
                    else
                        if((ss2.end_time - ss2.start_time)*1.00/3600 > 5.00 and (ss2.end_time - ss2.start_time)*1.00/3600 < 10.00, (ss2.end_time - 28800)/3600, ss2.start_time/3600)
                end as start_shift
                ,case
                    when coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) < date'2021-10-22' then
                        case
                            when ((sm.shipper_id in (9887416,4826244) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-08' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (16814977) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-09' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (4851147) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-10' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-14' and ss2.end_time is not null)
                                  ) then ss2.end_time/3600
                            when ((sm.shipper_id in (9887416,4826244) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-08' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (16814977) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-09' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (4851147) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-10' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-14' and ss2.end_time is null)
                                  ) then null
                            else ss1.end_time/3600
                        end
                    else
                        ss2.end_time/3600
                end as end_shift
                ,case
                    when ss2.registration_status = 1 then 'Registered'
                    when ss2.registration_status = 2 then 'Off'
                    when ss2.registration_status = 3 then 'Work'
                else
                    case
                        when coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) < date'2021-10-22' then null
                    else 'Not registered' end
                end as registration_status
                ,case
                    when coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) < date'2021-10-22' then
                        case
                            when ((sm.shipper_id in (9887416,4826244) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-08' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (16814977) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-09' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (4851147) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-10' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-14' and ss2.end_time is not null)
                                  ) then if(ss2.registration_status = 2, case
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Mon' then '1'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Tue' then '2'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Wed' then '3'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Thu' then '4'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Fri' then '5'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Sat' then '6'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Sun' then '7'
                                        end, null)
                            when ((sm.shipper_id in (9887416,4826244) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-08' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (16814977) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-09' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (4851147) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-10' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-14' and ss2.end_time is null)
                                  ) then case
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Mon' then '1'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Tue' then '2'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Wed' then '3'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Thu' then '4'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Fri' then '5'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Sat' then '6'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Sun' then '7'
                                        end
                            else ss1.off_weekdays
                        end
                    else
                        if(ss2.end_time is not null, if(ss2.registration_status = 2, case
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Mon' then '1'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Tue' then '2'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Wed' then '3'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Thu' then '4'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Fri' then '5'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Sat' then '6'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Sun' then '7'
                                        end, null), case
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Mon' then '1'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Tue' then '2'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Wed' then '3'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Thu' then '4'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Fri' then '5'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Sat' then '6'
                                            when date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a') = 'Sun' then '7'
                                        end)
                end as off_weekdays
                ,case
                    when coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) < date'2021-10-22' then
                        case
                            when ((sm.shipper_id in (9887416,4826244) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-08' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (16814977) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-09' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (4851147) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-10' and ss2.end_time is not null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-14' and ss2.end_time is not null)
                                  ) then if(ss2.registration_status = 2, date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a'), null)
                            when ((sm.shipper_id in (9887416,4826244) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-08' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (16814977) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-09' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (4851147) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-10' and ss2.end_time is null)
                                    or
                                  (sm.shipper_id in (20177915,19695656,19678726,19625407,19624015,19612631,19599250,19551831,19481510,19468915,19454405,19401151,19373710,19373061,19320304,19308777,19267854,19173195,19047576,18947308,18937575,18878956,18872504,18846132,18791517,18654923,18597974,18549757,18549417,18480486,18474136,18333920,17927517,17876027,17530242,17521624,17512097,17462160,17438584,17192320,17190998,17137668,17094280,16762533,16682481,16587104,16408221,16335219,16244300,16169098,15870834,15864561,15836872,15830057,15616524,15442391,15212630,14625540,14616073,14565744,14540545,14365003,14298055,14227878,14218985,13024234,12703121,12190936,12015530,11708187,11123445,10801049,10740544,10715586,10510273,10428154,10427228,10240099,10229682,10114563,9898813,9860279,9281040,9253801,9084601,8790025,8570230,8352484,7986485,7512837,7432930,6722139,6475666,5930893,4668512,4611002,4236199,3976901,3436247,3084715
                                  ) and coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))) >= date'2021-10-14' and ss2.end_time is null)
                                  ) then date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a')
                            else null
                        end
                    else
                        if(ss2.end_time is not null, if(ss2.registration_status = 2, date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a'), null)
                        , date_format(coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600))), '%a'))
                end as off_date
                ,array_join(array_agg(cast(d_.cha_date as VARCHAR)),', ') as off_date_1

                from (select * from shopeefood.foody_mart__profile_shipper_master where  grass_date != 'current' and  shipper_type_id = 12) sm
                left join shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss1 on ss1.id = sm.shipper_shift_id
                full join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live ss2 on ss2.uid = sm.shipper_id and date(from_unixtime(ss2.date_ts-3600)) = coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600)))
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
        GROUP BY 1,2,3,4,5,6,7,8,9,10
        )
    )
SELECT
    shipper_id
    ,shipper_type_id
    ,report_date
    ,shipper_shift_id
    ,COALESCE(start_shift, start_shift_previous) AS start_shift
    ,COALESCE(end_shift, end_shift_previous) AS end_shift
    ,off_weekdays
    ,COALESCE(registration_status, 'Not registered') AS registration_status
    ,off_date
FROM
    (SELECT
        b1.shipper_id
        ,b1.shipper_type_id
        ,b1.report_date
        ,b1.shipper_shift_id
        ,b1.start_shift
        ,b1.end_shift
        ,b1.off_weekdays
        ,b1.registration_status
        ,b1.off_date
        ,MAX_BY(b2.start_shift, b2.report_date) AS start_shift_previous
        ,MAX_BY(b2.end_shift, b2.report_date) AS end_shift_previous
    FROM base b1
    LEFT JOIN base b2 ON b1.report_date > b2.report_date AND b2.start_shift IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9)
WHERE report_date BETWEEN DATE_TRUNC('month', current_date - interval '1' day) - interval '2' month and current_date - interval '1' day
)
, online_time AS
(SELECT
    shipper_id
    , create_date AS report_date
    , CAST(SUM(DATE_DIFF('second', actual_start_time_online, actual_end_time_online)) AS DOUBLE) / 3600 AS total_online_time
    , CAST(SUM(DATE_DIFF('second', actual_start_time_work, actual_end_time_work)) AS DOUBLE) / 3600 AS total_working_time
FROM
    (SELECT
        uid AS shipper_id
        ,DATE(FROM_UNIXTIME(create_time - 3600)) AS create_date
        ,FROM_UNIXTIME(check_in_time - 3600) AS actual_start_time_online
        ,GREATEST(FROM_UNIXTIME(check_out_time - 3600),FROM_UNIXTIME(order_end_time - 3600)) AS actual_end_time_online
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
    AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2022-01-29' AND DATE'2022-02-06'
    )
GROUP BY 1,2
)
, assignment AS
(SELECT
    assign.date_ AS report_date
    , assign.shipper_id
    , COALESCE(assign.cnt_total_assign_order,0) + COALESCE(deny.cnt_deny_acceptable,0) AS cnt_total_assign_order
    , COALESCE(assign.cnt_total_incharge,0) AS cnt_total_incharge
    , COALESCE(assign.cnt_ignore_total,0) AS cnt_ignore_total
    , COALESCE(deny.cnt_deny_acceptable,0) AS cnt_deny_total
    FROM
        (SELECT
            date_
            , shipper_id
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
                , a.status
                , IF(a.experiment_group IN (3,4,7,8), 1, 0) AS is_auto_accepted
            FROM
                (SELECT
                    CONCAT(CAST(order_id AS VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                    , order_id, city_id, assign_type, update_time, create_time, status, order_type
                    , experiment_group, shipper_uid AS shipper_id

                FROM shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                WHERE status IN (3,4,8,9,17,18) -- shipper incharge + deny + ignore
                AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2022-01-29' AND DATE'2022-02-06'

                UNION ALL

                SELECT
                    CONCAT(CAST(order_id as VARCHAR), '-', CAST(order_type AS VARCHAR)) AS order_uid
                    , order_id, city_id, assign_type, update_time, create_time, status, order_type
                    , experiment_group, shipper_uid AS shipper_id

                FROM shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                WHERE status IN (3,4,8,9,17,18) -- shipper incharge + deny + ignore
                AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE'2022-01-29' AND DATE'2022-02-06'
                ) a
            )
        GROUP BY 1,2
        ) assign

    LEFT JOIN

        (SELECT
            deny_date
            , shipper_id
            , COUNT(ref_order_code) AS cnt_deny_total
            , COUNT(IF(deny_type = 'Driver_Fault', ref_order_code, NULL)) AS cnt_deny_acceptable
            , COUNT(IF(deny_type <> 'Driver_Fault', ref_order_code, NULL)) AS cnt_deny_non_acceptable
        FROM
            (SELECT
                dod.uid AS shipper_id
                , DATE(FROM_UNIXTIME(dod.create_time - 3600)) AS deny_date
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
            WHERE DATE(FROM_UNIXTIME(dod.create_time - 3600)) BETWEEN DATE'2022-01-29' AND DATE'2022-02-06'
            ) dod
        group by 1,2
        ) deny on assign.date_ = deny.deny_date AND assign.shipper_id = deny.shipper_id
)
, shift_orders AS 
(SELECT 
    base1.shipper_id
    ,base1.report_date
    ,count(distinct case when base1.order_status = 'Delivered' then base1.order_uid else null end ) AS cnt_delivered_order
    ,count(distinct case when base1.order_status = 'Delivered' and base1.is_order_in_hub_shift = 1 then base1.order_uid else null end ) AS cnt_delivered_order_in_shift
FROM
    (SELECT  
        *
        ,case 
            when base.report_date between date('2021-07-09') and date('2021-10-05') and is_hub_driver = 1 and base.city_id = 217 then 1
            when base.report_date between date('2021-07-24') and date('2021-10-04') and is_hub_driver = 1 and base.city_id = 218 then 1
            when base.driver_payment_policy = 2 then 1 else 0 
        end as is_order_in_hub_shift
    
    FROM
        (SELECT 
            dot.uid as shipper_id
            , dot.ref_order_id as order_id
            , dot.ref_order_code as order_code
            , dot.ref_order_category
            , dot.id
            ,CONCAT(CAST(dot.ref_order_id AS VARCHAR), '-', CAST(dot.ref_order_category AS VARCHAR)) AS order_uid
            ,case when dot.order_status = 400 then 'Delivered'
                when dot.order_status = 401 then 'Quit'
                when dot.order_status in (402,403,404) then 'Cancelled'
                when dot.order_status in (405,406,407) then 'Others'
                else 'Others' end as order_status
            ,dot.is_asap
            ,dot.pick_city_id as city_id
            ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
            ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
            ,case when driver_hub.shipper_type_id = 12 then 1 else 0 end as is_hub_driver
        FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot
        LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
        LEFT JOIN
            (SELECT  
                sm.shipper_id
                ,sm.shipper_type_id
                ,try_cast(sm.grass_date as date) as report_date
            from shopeefood.foody_mart__profile_shipper_master sm
            where 1=1
            and sm.grass_date != 'current'
            and shipper_type_id <> 3
            and shipper_status_code = 1
            ) driver_hub on driver_hub.shipper_id = dot.uid and driver_hub.report_date = case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                                                                                                 when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                                                                                                 else date(from_unixtime(dot.submitted_time- 60*60)) end
        WHERE 1=1
        and dot.pick_city_id <> 238
        and case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                else date(from_unixtime(dot.submitted_time- 60*60)) end between DATE_TRUNC('month', current_date - interval '1' day) - interval '2' month and current_date - interval '1' day
        ) base
    ) base1
GROUP BY 1,2
)
, kpi_qualified_tet AS
(SELECT
    s.shipper_id 
    , s.report_date 
    , o.total_online_time
    , a.cnt_total_incharge
    , a.cnt_ignore_total
    , a.cnt_deny_total
    , IF( COALESCE(CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600, o.total_online_time) >= 5
          AND TRY(CAST(COALESCE(a.cnt_total_incharge, 0) AS DOUBLE) / (COALESCE(a.cnt_total_incharge, 0) + COALESCE(a.cnt_ignore_total, 0) + COALESCE(a.cnt_deny_total, 0))) >= 0.9
        ,1,0) AS is_qualified_kpi_tet
FROM shift s
LEFT JOIN online_time o ON s.shipper_id = o.shipper_id AND s.report_date = o.report_date
LEFT JOIN assignment a ON s.shipper_id = a.shipper_id AND s.report_date = a.report_date
LEFT JOIN shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc  ON s.shipper_id = hc.uid AND s.report_date = DATE(FROM_UNIXTIME(hc.report_date - 3600))
WHERE s.report_date BETWEEN DATE'2022-01-29' AND DATE'2022-02-06'
)

, kpi_qualified_non_tet AS
(SELECT
    hc.uid AS shipper_id
    , DATE(FROM_UNIXTIME(hc.report_date - 3600)) AS report_date
    , CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) AS hub_shift
    , CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) AS deny_count
    , CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) AS ignore_count
    , CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 AS online_in_shift
    , CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 AS online_peak_hour
    , regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') AS is_auto_accept
    , case when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '10 hour shift'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/600 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.00000000/3600 >= 2 then 1
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '8 hour shift'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/485 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 2 then 1
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '5 hour shift'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/300 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 then 1
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HCM'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/180 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' then 1
--and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.000/3600 >= 1 then 1
when cast(json_extract(hc.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_group = 'HN'
and cast(json_extract(hc.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hc.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hc.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/180 >= 0.9
and array_join(cast(json_extract(hc.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hc.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 then 1
else 0  
    END AS is_qualified_kpi_non_tet
FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc
WHERE DATE(FROM_UNIXTIME(hc.report_date - 3600)) BETWEEN DATE_TRUNC('month', current_date - interval '1' day) - interval '2' month and current_date - interval '1' day
)
, base AS
(SELECT
    report_date
    , COUNT(DISTINCT shipper_id) AS total_hub_drivers
    , COUNT(DISTINCT IF(is_qualified_kpi = 1, shipper_id, NULL)) AS qualified_hub_drivers
FROM
    (SELECT
        s.report_date
        , s.shipper_id
        , CASE
            WHEN s.report_date BETWEEN DATE'2022-01-29' AND DATE'2022-02-06' THEN
                    CASE
                        WHEN s.registration_status IN ('Work', 'Registered') AND COALESCE(so.cnt_delivered_order, 0) > 0 AND COALESCE(tet.is_qualified_kpi_tet, 0) = 1 THEN 1
                ELSE 0 END
            WHEN COALESCE(so.cnt_delivered_order_in_shift, 0) > 0 AND COALESCE(non_tet.is_qualified_kpi_non_tet, 0) = 1 THEN 1
        ELSE 0 END AS is_qualified_kpi
    FROM shift s
    INNER JOIN (SELECT shipper_id, report_date FROM shift_orders WHERE cnt_delivered_order > 0 GROUP BY 1,2) a1 ON s.shipper_id = a1.shipper_id AND s.report_date = a1.report_date
    LEFT JOIN shift_orders so ON s.shipper_id = so.shipper_id AND s.report_date = so.report_date
    LEFT JOIN kpi_qualified_tet tet ON s.shipper_id = tet.shipper_id AND s.report_date = tet.report_date
    LEFT JOIN kpi_qualified_non_tet non_tet ON s.shipper_id = non_tet.shipper_id AND s.report_date = non_tet.report_date
    )
GROUP BY 1
)
---- FINALE
SELECT
    p.period
    , p.days AS days
    , SUM(b.total_hub_drivers) / p.days AS total_hub_drivers
    , SUM(b.qualified_hub_drivers) / p.days AS qualified_hub_drivers
    , TRY(CAST(SUM(b.qualified_hub_drivers) AS DOUBLE) / SUM(b.total_hub_drivers)) AS qualified_pct
FROM base b
INNER JOIN params p ON b.report_date BETWEEN p.start_date AND p.end_date
GROUP BY 1,2