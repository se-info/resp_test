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
, base AS (
SELECT
    base3.shipper_id
    ,base3.report_date AS grass_date
    ,base3.city_group
    ,case when driver_type.shipper_type_id = 1 then 'full-time'
        when driver_type.shipper_type_id = 3 then 'tester'
        when driver_type.shipper_type_id = 11 then 'part-time'
        when driver_type.shipper_type_id = 12 then 'hub'
        else 'part-time' end as current_shipper_type
    ,case when driver_type.start_shift = 0 and driver_type.end_shift = 23 then 'All-Day'
        when driver_type.end_shift - driver_type.start_shift = 10 then 'HUB10'
        when driver_type.end_shift - driver_type.start_shift = 8 then 'HUB08'
        when driver_type.end_shift - driver_type.start_shift = 5 and driver_type.start_shift < 11 then 'HUB-05S'
        when driver_type.end_shift - driver_type.start_shift = 5 and driver_type.start_shift > 11 then 'HUB-05C'
        else null end as hub_type
    ,case when driver_type.start_shift = 0 and driver_type.end_shift = 23 then 'All-Day'
        when driver_type.end_shift - driver_type.start_shift = 10 then 'HUB10'
        when driver_type.end_shift - driver_type.start_shift = 8 then 'HUB08'
        when driver_type.end_shift - driver_type.start_shift = 5 then 'HUB05'
        else null end as hub_type_v2

    -- overall
    ,base3.cnt_total_order_all
    ,base3.cnt_delivered_order_all
    ,base3.cnt_cancelled_order_all
    ,base3.cnt_quit_order_all
    ,base3.cnt_return_order_all

    ,base3.cnt_total_order_food_all
    ,base3.cnt_delivered_order_food_all
    ,base3.cnt_cancelled_order_food_all
    ,base3.cnt_quit_order_food_all

    ,base3.cnt_total_order_ns_all
    ,base3.cnt_delivered_order_ns_all
    ,base3.cnt_cancelled_order_ns_all

    --inshift
    ,base3.oct_cnt_total_order_in_shift
    ,base3.oct_cnt_delivered_order_in_shift
    ,base3.oct_cnt_cancelled_order_in_shift
    ,base3.oct_cnt_quit_order_in_shift

    ,base3.oct_cnt_total_order_in_shift_food
    ,base3.oct_cnt_delivered_order_in_shift_food
    ,base3.oct_cnt_cancelled_order_in_shift_food
    ,base3.oct_cnt_quit_order_in_shift_food

    ,base3.oct_cnt_total_order_in_shift_ns
    ,base3.oct_cnt_delivered_order_in_shift_ns
    ,base3.oct_cnt_cancelled_order_in_shift_ns
    ,base3.oct_cnt_returned_order_in_shift_ns

    ,base3.nonhub_delivered_orders
    ,base3.nonhub_total_orders
    ,base3.hub_delivered_orders
    ,base3.hub_total_orders

    ,base3.nonhub_lt_completion
    ,base3.nonhub_asap_delivered_orders
    ,base3.hub_lt_completion
    ,base3.hub_asap_delivered_orders
    ,base3.lt_completion
    ,base3.asap_delivered_orders

---- driver - total order completed, cancel, quit overview (inshift and overall)
FROM
        (SELECT
            base2.shipper_id
            ,base2.report_date
            ,base2.city_group
            ,cast(date_format(cast(base2.report_date as TIMESTAMP), '%a') as varchar) days_of_week

            -- in hub shift
            --overall
            ,count(distinct case when base2.is_order_in_hub_shift = 1 then base2.uid else null end) oct_cnt_total_order_in_shift
            ,count(distinct case when base2.is_del = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_delivered_order_in_shift
            ,count(distinct case when base2.is_cancel = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_cancelled_order_in_shift
            ,count(distinct case when base2.is_quit = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_quit_order_in_shift

            --food
            ,count(distinct case when base2.is_now_food = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end) oct_cnt_total_order_in_shift_food
            ,count(distinct case when base2.is_now_food = 1 and base2.is_del = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_delivered_order_in_shift_food
            ,count(distinct case when base2.is_now_food = 1 and base2.is_cancel = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_cancelled_order_in_shift_food
            ,count(distinct case when base2.is_now_food = 1 and base2.is_quit = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_quit_order_in_shift_food

            --ns
            ,count(distinct case when base2.is_now_food = 0 and base2.is_order_in_hub_shift = 1 then base2.uid else null end) oct_cnt_total_order_in_shift_ns
            ,count(distinct case when base2.is_now_food = 0 and base2.is_del = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_delivered_order_in_shift_ns
            ,count(distinct case when base2.is_now_food = 0 and base2.is_cancel = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_cancelled_order_in_shift_ns
            ,count(distinct case when base2.is_now_food = 0 and base2.is_return = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end ) oct_cnt_returned_order_in_shift_ns

            -- overall
            ,count(distinct base2.uid ) cnt_total_order_all
            ,count(distinct case when base2.is_del = 1 then base2.uid else null end ) cnt_delivered_order_all
            ,count(distinct case when base2.is_cancel = 1 then base2.uid else null end ) cnt_cancelled_order_all
            ,count(distinct case when base2.is_quit = 1 then base2.uid else null end ) cnt_quit_order_all
            ,count(distinct case when base2.is_return = 1 then base2.uid else null end ) cnt_return_order_all

            ,count(distinct case when base2.is_now_food = 1 then base2.uid else null end) cnt_total_order_food_all
            ,count(distinct case when base2.is_now_food = 1 and base2.is_del = 1 then base2.uid else null end ) cnt_delivered_order_food_all
            ,count(distinct case when base2.is_now_food = 1 and base2.is_cancel = 1 then base2.uid else null end ) cnt_cancelled_order_food_all
            ,count(distinct case when base2.is_now_food = 1 and base2.is_quit = 1 then base2.uid else null end ) cnt_quit_order_food_all

            ,count(distinct case when base2.is_now_food = 0 then base2.uid else null end) cnt_total_order_ns_all
            ,count(distinct case when base2.is_now_food = 0 and base2.is_del = 1 then base2.uid else null end ) cnt_delivered_order_ns_all
            ,count(distinct case when base2.is_now_food = 0 and base2.is_cancel = 1 then base2.uid else null end ) cnt_cancelled_order_ns_all

            ,count(distinct case when base2.is_now_food = 1 and (base2.is_order_delivered_by_driver_hub = 0 or base2.is_order_in_hub_shift = 0) and base2.is_del = 1 then base2.uid else null end) as nonhub_delivered_orders
            ,count(distinct case when base2.is_now_food = 1 and (base2.is_order_delivered_by_driver_hub = 0 or base2.is_order_in_hub_shift = 0) then base2.uid else null end) as nonhub_total_orders
            ,count(distinct case when base2.is_now_food = 1 and base2.is_order_delivered_by_driver_hub = 1 and base2.is_order_in_hub_shift = 1 and base2.is_del = 1 then base2.uid else null end) as hub_delivered_orders
            ,count(distinct case when base2.is_now_food = 1 and base2.is_order_delivered_by_driver_hub = 1 and base2.is_order_in_hub_shift = 1 then base2.uid else null end) as hub_total_orders

            ,sum(case when base2.is_now_food = 1 and (base2.is_order_delivered_by_driver_hub = 0 or base2.is_order_in_hub_shift = 0) and base2.is_asap = 1 and base2.is_del = 1 and base2.is_valid_submit_to_del = 1 then base2.lt_completion else 0 end) as nonhub_lt_completion
            ,count(distinct case when base2.is_now_food = 1 and (base2.is_order_delivered_by_driver_hub = 0 or base2.is_order_in_hub_shift = 0) and base2.is_asap = 1 and base2.is_del = 1 and base2.is_valid_submit_to_del = 1 then base2.uid else null end) as nonhub_asap_delivered_orders
            ,sum(case when base2.is_now_food = 1 and base2.is_order_delivered_by_driver_hub = 1 and base2.is_order_in_hub_shift = 1 and base2.is_asap = 1 and base2.is_del = 1 and base2.is_valid_submit_to_del = 1 then base2.lt_completion else 0 end) as hub_lt_completion
            ,count(distinct case when base2.is_now_food = 1 and base2.is_order_delivered_by_driver_hub = 1 and base2.is_order_in_hub_shift = 1 and base2.is_asap = 1 and base2.is_del = 1 and base2.is_valid_submit_to_del = 1 then base2.uid else null end) as hub_asap_delivered_orders
            ,sum(case when base2.is_now_food = 1 and base2.is_asap = 1 and base2.is_del = 1 and base2.is_valid_submit_to_del = 1 then base2.lt_completion else 0 end) as lt_completion
            ,count(distinct case when base2.is_now_food = 1 and base2.is_asap = 1 and base2.is_del = 1 and base2.is_valid_submit_to_del = 1 then base2.uid else null end) as asap_delivered_orders
        FROM
            (SELECT
                 base1.uid
                ,case when base1.source = 'order_delivery' then 'NowFood' else 'NowShip' end as source
                ,base1.shipper_id
                ,base1.city_group
                ,base1.report_date
                ,base1.order_status
                ,base1.is_hub_driver
                ,base1.is_order_in_hub_shift
                ,case when base1.order_status = 'Delivered' then 1 else 0 end as is_del
                ,case when base1.order_status = 'Cancelled'  then 1 else 0 end as is_cancel
                ,case when base1.order_status = 'Quit' then 1 else 0 end as is_quit
                ,case when base1.order_status = 'Returned' then 1 else 0 end as is_return
                ,case when base1.source = 'order_delivery' then 1 else 0 end as is_now_food
                ,coalesce(hub_info.id, 0) as h_id
                ,case when coalesce(hub_info.id, 0) > 0 and base1.is_hub_driver = 1 then 1 else 0 end as is_order_delivered_by_driver_hub
                ,base1.is_valid_submit_to_del
                ,base1.lt_completion
                ,base1.is_asap

            FROM
                (SELECT
                    base.shipper_id
                    ,base.report_date
                    ,base.city_group
                    ,concat(base.source,'_',cast(base.order_id as varchar)) as uid
                    ,base.ref_order_category as order_type
                    ,base.source

                    ,case when base.order_status = 400 then 'Delivered'
                        when base.order_status = 401 then 'Quit'
                        when base.order_status in (402,403,404) then 'Cancelled'
                        when base.order_status in (405,406,407) then 'Others'
                        else 'Others' end as order_status

                    ,base.is_hub_driver
                    ,base.hub_id
                    ,base.is_asap
                    ,case when base.created_timestamp <= base.last_delivered_timestamp then 1 else 0 end as is_valid_submit_to_del
                    ,cast(date_diff('second', base.created_timestamp, base.last_delivered_timestamp) as double) / 60 as lt_completion
                    ,case when base.report_date between date('2021-07-09') and date('2021-10-05') and is_hub_driver = 1 and base.city_id = 217 then 1
                         when base.report_date between date('2021-07-24') and date('2021-10-04') and is_hub_driver = 1 and base.city_id = 218 then 1
                         when base.driver_payment_policy = 2 then 1 else 0 end as is_order_in_hub_shift
                FROM
                    (SELECT
                        dot.uid as shipper_id
                        ,dot.ref_order_id as order_id
                        ,dot.ref_order_code as order_code
                        ,dot.ref_order_category
                        ,case when dot.ref_order_category = 0 then 'order_delivery'
                            when dot.ref_order_category = 3 then 'now_moto'
                            when dot.ref_order_category = 4 then 'now_ship'
                            when dot.ref_order_category = 5 then 'now_ship'
                            when dot.ref_order_category = 6 then 'now_ship_shopee'
                            when dot.ref_order_category = 7 then 'now_ship_sameday'
                            when dot.ref_order_category = 7 then 'now_ship_multi_drop'
                            else null end source
                        ,dot.ref_order_status
                        ,dot.order_status
                        ,case when dot.order_status = 1 then 'Pending'
                            when dot.order_status in (100,101,102) then 'Assigning'
                            when dot.order_status in (200,201,202,203,204) then 'Processing'
                            when dot.order_status in (300,301) then 'Error'
                            when dot.order_status in (400,401,402,403,404,405,406,407) then 'Completed'
                            else null end as order_status_group

                        ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 60*60))
                            when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 60*60))
                            else date(from_unixtime(dot.submitted_time- 60*60)) end as report_date
                        ,date(from_unixtime(dot.submitted_time- 60*60)) created_date
                        ,from_unixtime(dot.submitted_time- 60*60) created_timestamp
                        ,dot.submitted_time
                        ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 60*60) end as last_delivered_timestamp
                        ,case when dot.estimated_drop_time = 0 then null else from_unixtime(dot.estimated_drop_time - 60*60) end as estimated_delivered_time
                        ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
                        ,case when dot.pick_city_id = 217 then 'HCM'
                            when dot.pick_city_id = 218 then 'HN'
                            when dot.pick_city_id = 219 then 'DN'
                            ELSE 'OTH' end as city_group
                        ,dot.pick_city_id as city_id
                        ,case when driver_hub.shipper_type_id = 12 then 1 else 0 end as is_hub_driver
                        ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy
                        ,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
                        ,COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id
                        ,dot.is_asap
                    FROM shopeefood.foody_partner_db__driver_order_tab__reg_daily_s0_live dot

                    left join shopeefood.foody_partner_db__driver_order_extra_tab__reg_daily_s0_live dotet on dot.id = dotet.order_id
                    LEFT JOIN shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category
                    LEFT JOIN shopeefood.foody_partner_db__order_group_mapping_tab__reg_daily_s0_live ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                                        and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                                        and ogm_filter.create_time >  ogm.create_time
                    LEFT JOIN shopeefood.foody_partner_db__order_group_info_tab__reg_daily_s0_live ogi on ogi.id = ogm.group_id

                    LEFT JOIN shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

                    LEFT JOIN shopeefood.foody_delivery_db__district_tab__reg_daily_s0_live district on district.id = dot.pick_district_id

                    LEFT JOIN
                        (SELECT
                            sm.shipper_id
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
                    and ogm_filter.create_time is null
                    and dot.pick_city_id <> 238
                    ) base
                WHERE 1=1
                and base.created_date BETWEEN DATE_TRUNC('month', current_date - interval '1' day) - interval '2' month AND current_date - interval '1' day
                and base.order_status_group = 'Completed'
                ) base1
            LEFT JOIN
                (SELECT
                    id
                    , hub_name
                    ,case when city_id = 217 then 'HCM'
                        when city_id = 218 then 'HN'
                        when city_id = 219 then 'DN'
                        ELSE 'OTH' end as hub_location
                    FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live
                    WHERE 1=1
                    and id <> 2
                    and driver_count > 0
                    and grass_schema = 'foody_internal_db'
                ) hub_info on hub_info.id = base1.hub_id
            ) base2
        GROUP BY 1,2,3,4
        ) base3

---- driver shift, start shift & end shift time , off_date , driver type in each report_date
LEFT JOIN
            (
            SELECT
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
            )driver_type on driver_type.shipper_id = base3.shipper_id and driver_type.report_date = base3.report_date
)

SELECT
    p.period
    , p.days AS days
    , SUM(b.cnt_delivered_order_food_all) / p.days AS vn_net_nowfood_ado
    , SUM(IF(b.city_group = 'HCM', b.cnt_delivered_order_food_all, 0)) / p.days AS hcm_net_nowfood_ado
    , SUM(IF(b.city_group = 'HN', b.cnt_delivered_order_food_all, 0)) / p.days AS hn_net_nowfood_ado
    , SUM(b.oct_cnt_delivered_order_in_shift) / p.days AS vn_net_nowfood_hub_ado
    , SUM(IF(b.city_group = 'HCM', b.oct_cnt_delivered_order_in_shift, 0)) / p.days AS hcm_net_nowfood_hub_ado
    , SUM(IF(b.city_group = 'HN', b.oct_cnt_delivered_order_in_shift, 0)) / p.days AS hn_net_nowfood_hub_ado
    , COUNT(DISTINCT IF(CASE
                                            WHEN b.grass_date BETWEEN DATE'2022-01-29' AND DATE'2022-02-06' THEN b.cnt_delivered_order_all > 0 AND b.current_shipper_type = 'hub'
                                            ELSE b.oct_cnt_delivered_order_in_shift > 0 END, (b.shipper_id, b.grass_date), NULL)) / p.days AS vn_a1_active_hub_drivers
    , COUNT(DISTINCT IF(CASE
                                            WHEN b.grass_date BETWEEN DATE'2022-01-29' AND DATE'2022-02-06' THEN b.cnt_delivered_order_all > 0 AND b.current_shipper_type = 'hub'
                                            ELSE b.oct_cnt_delivered_order_in_shift > 0 END AND b.city_group = 'HCM', (b.shipper_id, b.grass_date), NULL)) / p.days AS _hcm_a1_active_hub_drivers
    , COUNT(DISTINCT IF(CASE
                                            WHEN b.grass_date BETWEEN DATE'2022-01-29' AND DATE'2022-02-06' THEN b.cnt_delivered_order_all > 0 AND b.current_shipper_type = 'hub'
                                            ELSE b.oct_cnt_delivered_order_in_shift > 0 END AND b.city_group = 'HN', (b.shipper_id, b.grass_date), NULL)) / p.days AS _hn_a1_active_hub_drivers
    , COUNT(DISTINCT IF(b.oct_cnt_delivered_order_in_shift > 0 AND hub_type_v2 = 'HUB10', (b.shipper_id, b.grass_date), NULL)) / p.days AS vn_a1_active_hub10_drivers
    , COUNT(DISTINCT IF(b.oct_cnt_delivered_order_in_shift > 0 AND hub_type_v2 = 'HUB08', (b.shipper_id, b.grass_date), NULL)) / p.days AS vn_a1_active_hub8_drivers
    , COUNT(DISTINCT IF(b.oct_cnt_delivered_order_in_shift > 0 AND hub_type_v2 = 'HUB05', (b.shipper_id, b.grass_date), NULL)) / p.days AS vn_a1_active_hub5_drivers
    , TRY(SUM(b.lt_completion) / SUM(b.asap_delivered_orders)) AS ata
    , TRY(SUM(b.nonhub_lt_completion) / SUM(b.nonhub_asap_delivered_orders)) AS nonhub_ata
    , TRY(SUM(b.hub_lt_completion) / SUM(b.hub_asap_delivered_orders)) AS hub_ata
    , TRY(CAST(SUM(b.cnt_delivered_order_food_all) AS DOUBLE) / sum(b.cnt_total_order_food_all)) AS g2n
    , TRY(CAST(SUM(b.nonhub_delivered_orders) AS DOUBLE) / SUM(b.nonhub_total_orders)) AS nonhub_g2n
    , TRY(CAST(SUM(b.hub_delivered_orders) AS DOUBLE) / SUM(b.hub_total_orders)) AS hub_g2n
FROM base b
INNER JOIN params p ON b.grass_date BETWEEN p.start_date AND p.end_date
GROUP BY 1,2
