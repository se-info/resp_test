WITH shift AS
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
        SELECT  coalesce(sm.shipper_id, ss2.uid) as shipper_id
                ,sm.shipper_name
                ,sm.shipper_type_id
                ,coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600)))as report_date
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

                from (SELECT * FROM shopeefood.foody_mart__profile_shipper_master WHERE grass_date != 'current' and shipper_type_id = 12 and city_id IN (217, 218)) sm
                full join shopeefood.foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live ss2 on ss2.uid = sm.shipper_id and date(from_unixtime(ss2.date_ts-3600)) = coalesce(try_cast(sm.grass_date as date), date(from_unixtime(ss2.date_ts-3600)))
                left join shopeefood.foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss1 on ss1.id = sm.shipper_shift_id
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
    ,registration_status
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
WHERE report_date BETWEEN DATE_TRUNC('week', current_date) - interval '7' day AND DATE_TRUNC('week', current_date) - interval '1' day
)
, kpi_qualified AS
(SELECT
    hub.uid AS shipper_id
    , DATE(FROM_UNIXTIME(hub.report_date - 3600)) AS report_date
    , CAST(json_extract(hub.extra_data,'$.shift_category_name') AS varchar) AS hub_shift
    , CAST(json_extract(hub.extra_data,'$.stats.deny_count') AS BIGINT) AS deny_count
    , CAST(json_extract(hub.extra_data,'$.stats.ignore_count') AS BIGINT) AS ignore_count
    , CAST(json_extract(hub.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 AS online_in_shift
    , CAST(json_extract(hub.extra_data,'$.stats.online_peak_hour') AS DOUBLE) / 3600 AS online_peak_hour
    , regexp_like(array_join(CAST(json_extract(extra_data,'$.passed_conditions') AS ARRAY<int>) ,','), '6') AS is_auto_accept
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600) start_shift
    ,from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600) end_shift
    ,date_diff('second',from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[1] as bigint)-3600)
            , from_unixtime(cast(cast(json_extract(extra_data,'$.shift_time_range') as array(json))[2] as bigint)-3600))/3600.00 as time_in_shift
--- KPI
,case when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '10 hour shift'
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/600 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.00000000/3600 >= 2 then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '8 hour shift'
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/485 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 2 then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '5 hour shift' and HOUR(from_unixtime(cast(json_extract(hub.extra_data,'$.shift_time_range[0]') as bigint) - 3600)) <> 6
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/300 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '5 hour shift' and HOUR(from_unixtime(cast(json_extract(hub.extra_data,'$.shift_time_range[0]') as bigint) - 3600)) = 6
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/300 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%'
-- and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 
then 1

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_id = 217   
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.00000000/60)*1.0000000/180 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' then 1 
--and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.000/3600 >= 1 then 1 

when cast(json_extract(hub.extra_data,'$.shift_category_name') as varchar) = '3 hour shift' and city_id = 218
and cast(json_extract(hub.extra_data,'$.stats.deny_count') as BIGINT) = 0
and cast(json_extract(hub.extra_data,'$.stats.ignore_count') as BIGINT) = 0
and (cast(json_extract(hub.extra_data,'$.stats.online_in_shift') as BIGINT)*1.0000000/60)*1.0000000/180 >= 0.9
and array_join(cast(json_extract(hub.extra_data,'$.passed_conditions') as array<int>) ,',') like '%6%' 
and cast(json_extract(hub.extra_data,'$.stats.online_peak_hour') as BIGINT)*1.0000000/3600 >= 1 then 1 

else 0 end as is_qualified_kpi

FROM shopeefood.foody_internal_db__shipper_hub_income_report_tab__reg_daily_s0_live hub
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm on sm.shipper_id = hub.uid and try_cast(sm.grass_date as date) =  DATE(FROM_UNIXTIME(hub.report_date - 3600))

WHERE DATE(FROM_UNIXTIME(hub.report_date - 3600)) BETWEEN DATE_TRUNC('week', current_date) - interval '7' day AND DATE_TRUNC('week', current_date) - interval '1' day
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
    AND DATE(FROM_UNIXTIME(create_time - 3600)) BETWEEN DATE_TRUNC('week', current_date) - interval '7' day AND DATE_TRUNC('week', current_date) - interval '1' day
    )
GROUP BY 1,2
)
, driver_level AS
(SELECT
    t.shipper_id
    , sm.shipper_name
    , sm.city_name
    , t.report_date
    , s.off_weekdays
    , s.off_date
    , s.shipper_shift_id
    , CAST(CONCAT(CAST(s.report_date as varchar) , ' ', CAST(s.start_shift AS VARCHAR), ':00:00') AS TIMESTAMP) AS start_shift
    , CAST(CONCAT(CAST(s.report_date as varchar) , ' ', CAST(s.end_shift AS VARCHAR), ':00:00') AS TIMESTAMP) AS end_shift
    , COALESCE(s.registration_status, 'Not registered') AS registration_status
    , k.is_qualified_kpi
    , k.hub_shift
    , k.deny_count
    , k.ignore_count
    , k.online_in_shift
    , k.online_peak_hour
    , k.is_auto_accept AS auto_accept
    , IF(t.report_date BETWEEN DATE'2022-01-29' AND DATE'2022-02-06', COALESCE(shift_orders.cnt_delivered_order, 0), COALESCE(shift_orders.cnt_delivered_order_in_shift, 0)) AS in_shift_delivered_orders
    , CASE
            WHEN t.report_date BETWEEN DATE'2022-01-29' AND DATE'2022-02-06' THEN
                    CASE
                        WHEN COALESCE(k.online_in_shift, ot.total_online_time) >= 5 AND TRY(CAST(COALESCE(a.cnt_total_incharge, 0) AS DOUBLE) / (COALESCE(a.cnt_total_incharge, 0) + COALESCE(a.cnt_ignore_total, 0) + COALESCE(a.cnt_deny_total, 0))) >= 0.9 AND COALESCE(s.registration_status, 'Not registered') IN ('Work', 'Registered') AND shift_orders.cnt_delivered_order > 0
                            THEN 1
                        ELSE 0 END
            ELSE IF(k.is_qualified_kpi = 1 AND COALESCE(shift_orders.cnt_delivered_order_in_shift, 0) > 0, 1, 0)
    END AS is_eligible_daily
    , IF(IF(t.report_date BETWEEN DATE'2022-01-29' AND DATE'2022-02-06', COALESCE(s.registration_status, 'Not registered') IN('Work', 'Registered'), IF(t.report_date BETWEEN DATE'2022-01-29' AND DATE'2022-02-06', COALESCE(shift_orders.cnt_delivered_order, 0), COALESCE(shift_orders.cnt_delivered_order_in_shift, 0)) > 0 OR COALESCE(s.registration_status, 'Not registered') IN('Work', 'Registered')), 1, 0) is_registered_daily
    ,a.cnt_total_incharge
    ,a.cnt_ignore_total
    ,a.cnt_deny_total
    ,TRY(CAST(COALESCE(a.cnt_total_incharge, 0) AS DOUBLE) / (COALESCE(a.cnt_total_incharge, 0) + COALESCE(a.cnt_ignore_total, 0) + COALESCE(a.cnt_deny_total, 0))) AS sla
    ,ot.total_online_time
FROM (SELECT s.shipper_id, r.report_date
    FROM
        (SELECT shipper_id
        FROM shift
        GROUP BY 1) s

    CROSS JOIN

        (SELECT report_date
        FROM ((SELECT sequence(DATE_TRUNC('week', current_date) - interval '7' day, DATE_TRUNC('week', current_date) - interval '1' day) bar)
            CROSS JOIN
            unnest (bar) as t(report_date)
            )
        ) r
    ) t
LEFT JOIN shift s ON t.shipper_id = s.shipper_id AND t.report_date = s.report_date
LEFT JOIN shopeefood.foody_mart__profile_shipper_master sm ON t.shipper_id = sm.shipper_id
LEFT JOIN kpi_qualified k ON t.shipper_id = k.shipper_id AND t.report_date = k.report_date
LEFT JOIN assignment a ON t.shipper_id = a.shipper_id AND t.report_date = a.report_date
LEFT JOIN online_time ot ON t.shipper_id = ot.shipper_id AND t.report_date = ot.report_date
LEFT JOIN (
        SELECT base1.shipper_id
              ,base1.report_date
              ,count(distinct case when base1.order_status = 'Delivered' then base1.order_uid else null end ) cnt_delivered_order
              ,count(distinct case when base1.order_status = 'Delivered' and base1.is_order_in_hub_shift = 1 then base1.order_uid else null end ) cnt_delivered_order_in_shift
        FROM
                (
                SELECT  *
                       ,case when base.report_date between date('2021-07-09') and date('2021-10-05') and is_hub_driver = 1 and base.city_id = 217 then 1
                             when base.report_date between date('2021-07-24') and date('2021-10-04') and is_hub_driver = 1 and base.city_id = 218 then 1
                             when base.driver_payment_policy = 2 then 1 else 0 end as is_order_in_hub_shift

                FROM
                            (
                            SELECT dot.uid as shipper_id
                                  ,dot.ref_order_id as order_id
                                 ,concat(cast(dot.ref_order_id as varchar), '-', cast(dot.ref_order_category as varchar)) as order_uid
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
                                  ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
                                        when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
                                        else date(from_unixtime(dot.submitted_time- 3600)) end as report_date
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
                                    )driver_hub on driver_hub.shipper_id = dot.uid and driver_hub.report_date = case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
                                                                                                                     when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
                                                                                                                     else date(from_unixtime(dot.submitted_time- 3600)) end
                            WHERE 1=1
                            and dot.pick_city_id <> 238
                            )base
                WHERE 1=1
                and base.report_date BETWEEN DATE_TRUNC('week', current_date) - interval '7' day AND DATE_TRUNC('week', current_date) - interval '1' day
                )base1
                GROUP BY 1,2
        ) shift_orders on t.shipper_id = shift_orders.shipper_id and t.report_date = shift_orders.report_date
WHERE sm.grass_date = 'current'
)
, daily_level AS
(SELECT
    shipper_id
    , shipper_name
    , city_name
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
    , IF(is_eligible_daily = 1 AND is_registered_daily = 1, 1, 0) AS is_eligible_daily
    , is_registered_daily
--     , IF(is_eligible_daily = 1 AND is_registered_daily = 1, 1, 0) AS eligible_days
    , cnt_total_incharge
    , cnt_ignore_total
    , cnt_deny_total
    , sla
    , total_online_time
    , SUM(IF(is_eligible_daily = 1 AND is_registered_daily = 1, 1, 0)) OVER (PARTITION BY shipper_id ORDER BY report_date) AS eligible_days
    , SUM(is_registered_daily) OVER (PARTITION BY shipper_id ORDER BY report_date) AS registration_days
FROM driver_level
WHERE 1=1
)

SELECT
    shipper_id
    , shipper_name
    , city_name
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
    , is_eligible_daily
    , is_registered_daily
    , IF(eligible_days = registration_days AND eligible_days > 0, 1, 0) AS eligible_days
    , IF(report_date >= current_date, NULL, COUNT(IF(in_shift_delivered_orders > 0, report_date, NULL)) OVER (PARTITION BY shipper_id ORDER BY report_date)) AS in_shift_delivered_orders_days
    , registration_days AS total_eligible_days
    , cnt_total_incharge
    , cnt_ignore_total
    , cnt_deny_total
    , sla
    , total_online_time
    , IF(report_date >= current_date, NULL, SUM(in_shift_delivered_orders) OVER (PARTITION BY shipper_id ORDER BY report_date)) AS cumulative_in_shift_delivered_orders
FROM daily_level
WHERE city_name IN ('HCM City', 'Ha Noi City')

/*
2022.05.03: update logic check online inshift kpi| shipper_id 21975529 > online time 89.6% 
remove AND CAST(json_extract(hc.extra_data,'$.stats.online_in_shift') AS DOUBLE) / 3600 / 5 >= 0.9
*/