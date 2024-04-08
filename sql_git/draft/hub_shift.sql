SELECT  shipper_id
        ,shipper_type_id
        ,report_date
        ,shipper_shift_id
        ,start_shift
        ,end_shift
        ,off_weekdays
        ,registration_status
        ,if(off_date is null and registration_status is null, off_date_1, off_date) as off_date
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
            else null end as registration_status
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
                    if(ss2.end_time is not null, case
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Mon' then '1'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Tue' then '2'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Wed' then '3'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Thu' then '4'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Fri' then '5'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Sat' then '6'
                                        when date_format(try_cast(sm.grass_date as date), '%a') = 'Sun' then '7'
                                    end, ss1.off_weekdays)
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
                    , null)
            end as off_date
            ,array_join(array_agg(cast(d_.cha_date as VARCHAR)),', ') as off_date_1

            from foody_mart__profile_shipper_master sm
            left join foody_internal_db__shipper_shift_tab__reg_daily_s0_live ss1 on ss1.id = sm.shipper_shift_id
            left join foody_internal_db__shipper_slot_registration_tab__vn_daily_s0_live ss2 on ss2.uid = sm.shipper_id and date(from_unixtime(ss2.date_ts-3600)) = try_cast(sm.grass_date as date)


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

                      FROM foody_internal_db__shipper_shift_tab__reg_daily_s0_live
                      WHERE 1=1
                      and off_weekdays in ('1','2','3','4','5','6','7')
                      GROUP BY 1,2
                     )d_ on regexp_like(ss1.off_weekdays,cast(d_.num_date  as varchar)) = true

            where 1=1
            and sm.grass_region = 'VN'
            and sm.grass_date != 'current'
            GROUP BY 1,2,3,4,5,6,7,8,9
    )