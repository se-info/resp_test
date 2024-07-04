WITH shift AS
(
    SELECT
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
        SELECT  sm.shipper_id
                ,sm.shipper_name
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
        and TRY_CAST(sm.grass_date AS DATE) BETWEEN DATE'2021-11-10' AND DATE'2021-11-16'
        and sm.shipper_type_id = 12
        GROUP BY 1,2,3,4,5,6,7,8,9,10
        )
    )
, kpi_qualified AS
(SELECT
    hc.uid AS shipper_id
    , DATE(FROM_UNIXTIME(hc.report_date - 3600)) AS report_date
    , CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) AS hub_shift
    , CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) AS deny_count
    , CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) AS ignore_count
    , CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 AS online_in_shift
    , CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 AS online_peak_hour
    , regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') AS is_auto_accept
    , CASE
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '10 hour shift'
            AND CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 / 10 >= 0.9
            AND CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 >= 2
            AND regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') THEN 1
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '8 hour shift'
            AND CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 / 8 >= 0.9
            AND CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 >= 2
            AND regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') THEN 1
        WHEN CAST(json_extract(hc.extra_data,'$.shift_category_name') AS varchar) = '5 hour shift'
            AND CAST(json_extract(hc.extra_data,'$.stats.deny_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.ignore_count') AS BIGINT) = 0
            AND CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 / 5 >= 0.9
            AND CAST(json_extract(hc.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 >= 1 
            AND regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') THEN 1
        ELSE 0
    END AS is_qualified_kpi
FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hc
WHERE DATE(FROM_UNIXTIME(hc.report_date - 3600)) BETWEEN DATE'2021-11-10' AND DATE'2021-11-16'
)
, log_hub as
(
SELECT uid as shipper_id , change_type, ref_id as hub_id, from_unixtime(create_time - 60*60) as create_ts, create_uid
       , case when uid in (8618999,2996387,3437959,16513079,13183218,19269423,14012684,18718560,4826244,10403079) and date(from_unixtime(create_time - 60*60)) between date('2021-09-20') and date('2021-09-30') then 1
              when uid in (15213402) and ref_id not in (26,35) and date(from_unixtime(create_time - 60*60)) between date('2021-09-20') and date('2021-09-30') then 1
              when uid in (3805328) and date(from_unixtime(create_time - 60*60)) between date('2021-09-21') and date('2021-09-30') then 1
              else 0 end as is_test
FROM
        (
        SELECT uid, change_type, ref_id, create_time, create_uid
        FROM shopeefood.foody_internal_db__shipper_change_log_tab__reg_daily_s0_live
        WHERE change_type in (4,5)

        UNION

        SELECT uid, change_type, ref_id, create_time, create_uid
        FROM shopeefood.foody_partner_db__shipper_change_log_tab__reg_daily_s0_live
        WHERE change_type in (4,5)
        )log_hub

WHERE 1=1
GROUP BY 1,2,3,4,5
)
,log_hub_v1 as
(
SELECT *
FROM
        (
        SELECT shipper_id, change_type, hub_id, create_ts, create_uid
              ,row_number() over(partition by concat(cast(shipper_id as VARCHAR),cast(change_type as varchar),cast(date(create_ts) as varchar)) order by create_ts desc) row_num
        FROM log_hub
        WHERE change_type = 4 and is_test = 0

        UNION

        SELECT shipper_id, change_type, hub_id, create_ts, create_uid
              ,row_number() over(partition by concat(cast(shipper_id as VARCHAR),cast(change_type as varchar),cast(date(create_ts) as varchar)) order by create_ts asc) row_num
        FROM log_hub
        WHERE change_type = 5 and is_test = 0
        )
WHERE row_num = 1
)
, revised_log as
(
SELECT shipper_id
      ,hub_current_start
      ,hub_current_join_time
      ,hub_current_end
      ,hub_curent_drop_time

FROM
(
(       SELECT * FROM
        (
        SELECT t1.shipper_id
              ,t1.change_type as change_type_from
              ,t1.hub_id as hub_current_start
              ,t1.create_ts as hub_current_join_time

              ,t2.change_type as change_type_to
              ,case when t2.hub_id > 0 then t2.hub_id else t1.hub_id end as hub_current_end
              ,case when t2.hub_id > 0 then t2.create_ts else localtimestamp - interval '1' hour end as hub_curent_drop_time

              ,row_number() over(partition by concat(cast(t1.shipper_id as varchar),cast(t1.hub_id as varchar),cast(t1.create_ts as varchar)) order by t2.create_ts ASC ) rank
        FROM (SELECT * FROM log_hub_v1 WHERE change_type = 4) t1

        LEFT JOIN (SELECT * FROM log_hub_v1 WHERE change_type = 5) t2 on t1.shipper_id = t2.shipper_id and t1.create_ts < t2.create_ts and t1.hub_id = t2.hub_id
        WHERE 1=1
        )base

        WHERE rank = 1

)
UNION

(       SELECT * FROM
        (
        SELECT t1.shipper_id
              ,t2.change_type as change_type_from

              ,case when t2.hub_id > 0 then t2.hub_id else t1.hub_id end as hub_current_start
              ,case when t2.hub_id > 0 then t2.create_ts else cast(date('2021-05-27') as TIMESTAMP) end as hub_current_join_time

              ,t1.change_type as change_type_to
              ,t1.hub_id as hub_current_end
              ,t1.create_ts as hub_curent_drop_time

              ,row_number() over(partition by concat(cast(t1.shipper_id as varchar),cast(t1.hub_id as varchar),cast(t1.create_ts as varchar)) order by t2.create_ts DESC ) rank

        FROM (SELECT * FROM log_hub_v1 WHERE change_type = 5) t1

        LEFT JOIN (SELECT * FROM log_hub_v1 WHERE change_type = 4) t2 on t1.shipper_id = t2.shipper_id and t1.create_ts > t2.create_ts and t1.hub_id = t2.hub_id
        )base1

        WHERE rank = 1

)
)base
WHERE 1=1
GROUP BY 1,2,3,4,5
)
, driver_level AS
(SELECT
    t.shipper_id
    , sm.shipper_name
    , t.report_date
    , s.off_weekdays
    , s.off_date
    , s.shipper_shift_id
    , CAST(CONCAT(CAST(s.report_date as varchar) , ' ', CAST(s.start_shift AS VARCHAR), ':00:00') AS TIMESTAMP) AS start_shift
    , CAST(CONCAT(CAST(s.report_date as varchar) , ' ', CAST(s.end_shift AS VARCHAR), ':00:00') AS TIMESTAMP) AS end_shift
    , s.registration_status
    , k.is_qualified_kpi
    , k.hub_shift
    , k.deny_count
    , k.ignore_count
    , k.online_in_shift
    , k.online_peak_hour
    , k.is_auto_accept AS auto_accept
    , COALESCE(shift_orders.cnt_delivered_order_in_shift, 0) AS in_shift_delivered_orders
    , IF(k.is_qualified_kpi = 1 AND COALESCE(shift_orders.cnt_delivered_order_in_shift, 0) > 0, 1, 0) AS is_eligible_daily
FROM (SELECT s.shipper_id, r.report_date
    FROM
        (SELECT shipper_id
        FROM shift
        GROUP BY 1) s

    CROSS JOIN

        (SELECT report_date
        FROM ((SELECT sequence(DATE'2021-11-10', DATE'2021-11-16') bar)
            CROSS JOIN
            unnest (bar) as t(report_date)
            )
        ) r
    ) t
LEFT JOIN shift s ON t.shipper_id = s.shipper_id AND t.report_date = s.report_date
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON t.shipper_id = sm.shipper_id
LEFT JOIN kpi_qualified k ON s.shipper_id = k.shipper_id AND s.report_date = k.report_date
LEFT JOIN (
        SELECT base1.shipper_id
              ,base1.report_date
              ,count(distinct case when base1.is_order_in_hub_shift = 1 then base1.uid else null end ) cnt_total_order_in_shift
              ,count(distinct case when base1.order_status = 'Delivered' and base1.is_order_in_hub_shift = 1 then base1.uid else null end ) cnt_delivered_order_in_shift
        FROM
                (
                SELECT  *
                       ,concat(base.source,'_',cast(base.id as varchar)) as uid
                       ,case when base.report_date between date('2021-07-09') and date('2021-10-05') and is_hub_driver = 1 and base.city_id = 217 then 1
                             when base.report_date between date('2021-07-24') and date('2021-10-04') and is_hub_driver = 1 and base.city_id = 218 then 1
                             when base.driver_payment_policy = 2 then 1 else 0 end as is_order_in_hub_shift

                FROM
                            (
                            SELECT dot.uid as shipper_id
                                  ,dot.ref_order_id as order_id
                                  ,dot.ref_order_code as order_code
                                  ,dot.ref_order_category
                                  ,dot.id
                                  ,case when dot.ref_order_category = 0 then 'NowFood'
                                        else 'NowShip' end source
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
                            left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
                            LEFT JOIN
                                    (
                                     SELECT  sm.shipper_id
                                            ,sm.shipper_type_id
                                            ,case when sm.grass_date = 'current' then date(current_date)
                                                else cast(sm.grass_date as date) end as report_date

                                            from shopeefood.foody_mart__profile_shipper_master sm

                                            where 1=1
                                            and shipper_type_id <> 3
                                            and shipper_status_code = 1
                                            and grass_region = 'VN'
                                            GROUP BY 1,2,3
                                    )driver_hub on driver_hub.shipper_id = dot.uid and driver_hub.report_date = case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                                                                                                                     when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                                                                                                                     else date(from_unixtime(dot.submitted_time- 60*60)) end
                            WHERE 1=1
                            and dot.pick_city_id <> 238
                            )base
                WHERE 1=1
                and base.report_date BETWEEN DATE'2021-11-10' AND DATE'2021-11-16'
                )base1
                GROUP BY 1,2
        ) shift_orders on s.shipper_id = shift_orders.shipper_id and s.report_date = shift_orders.report_date
WHERE sm.grass_date = 'current'
)
, daily_level AS
(SELECT
    shipper_id
    , shipper_name
    , report_date
    , hub_shift
    , off_weekdays
    , off_date
    , shipper_shift_id
    , start_shift
    , end_shift
    , registration_status
    , is_qualified_kpi
    , deny_count
    , ignore_count
    , online_in_shift
    , online_peak_hour
    , auto_accept
    , in_shift_delivered_orders
    , is_eligible_daily
    , SUM(IF(in_shift_delivered_orders > 0, is_eligible_daily, 0)) OVER (PARTITION BY shipper_id ORDER BY report_date ASC) AS eligible_days
    , COUNT(IF(in_shift_delivered_orders > 0, report_date, NULL)) OVER (PARTITION BY shipper_id ORDER BY report_date ASC) AS inshift_days
    , COUNT(IF(in_shift_delivered_orders > 0 OR registration_status IN ('Registered', 'Worked'), report_date, NULL)) OVER (PARTITION BY shipper_id ORDER BY report_date ASC) AS registered_days
FROM driver_level
WHERE 1=1
)

SELECT
    shipper_id
    , shipper_name
    , report_date
    , hub_shift
    , off_weekdays
    , off_date
    , shipper_shift_id
    , start_shift
    , end_shift
    , registration_status
    , is_qualified_kpi
    , deny_count
    , ignore_count
    , online_in_shift
    , online_peak_hour
    , IF(report_date <= current_date - interval '1' day, auto_accept, NULL) AS auto_accept
    , in_shift_delivered_orders
    , IF(report_date <= current_date - interval '1' day AND eligible_days = registered_days, 1, 0) AS is_eligible
    , IF(report_date <= current_date - interval '1' day, eligible_days, NULL) AS eligible_days
    , IF(report_date <= current_date - interval '1' day, inshift_days, NULL) AS inshift_days
    , IF(report_date <= current_date - interval '1' day, registered_days, NULL) AS registered_days
FROM daily_level