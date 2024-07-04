WITH params(period, start_date, end_date, days) AS (
    VALUES
    (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day), '%b'), DATE_TRUNC('month', current_date - interval '1' day), current_date - interval '1' day, CAST(DAY(current_date - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, '%b'), DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month, DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day, CAST(DAY(DATE_TRUNC('month', current_date - interval '1' day) - interval '1' day) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day), 'W%v'), DATE_TRUNC('week', current_date - interval '1' day), current_date - interval '1' day, CAST(DATE_DIFF('day', DATE_TRUNC('week', current_date - interval '1' day), current_date) AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '7' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '7' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '1' day, CAST(7 AS DOUBLE))
    , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '14' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '14' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '8' day, CAST(7 AS DOUBLE))
    -- , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '21' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '21' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '15' day, CAST(7 AS DOUBLE))
    -- , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '28' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '28' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '22' day, CAST(7 AS DOUBLE))
    -- , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '35' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '35' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '29' day, CAST(7 AS DOUBLE))
    -- , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '42' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '42' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '36' day, CAST(7 AS DOUBLE))
    -- , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '49' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '49' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '43' day, CAST(7 AS DOUBLE))
    -- , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '56' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '56' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '50' day, CAST(7 AS DOUBLE))
    -- , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '63' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '63' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '57' day, CAST(7 AS DOUBLE))
    -- , (DATE_FORMAT(DATE_TRUNC('week', current_date - interval '1' day) - interval '70' day, 'W%v'), DATE_TRUNC('week', current_date - interval '1' day) - interval '70' day, DATE_TRUNC('week', current_date - interval '1' day) - interval '64' day, CAST(7 AS DOUBLE))
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
SELECT    base3.source
          ,base3.source_2
          ,base3.food_service
          ,base3.created_date AS grass_date
          ,base3.city_group
          ,base3.city_name
          ,base3.is_order_in_hub_shift
          ,base3.is_order_delivered_by_driver_hub
          ,base3.is_hub_driver
          ,base3.is_stack_order
          ,base3.is_group_order

          ,sum(base3.total_order) total_order
          ,sum(case when base3.is_del = 1 and base3.is_valid_submit_to_del = 1 then base3.total_order else 0 end) total_order_delivered
          ,sum(case when base3.is_del = 1 and base3.is_late_delivered_time_eta_max = 1 and base3.is_valid_submit_to_del = 1 then base3.total_order else 0 end) total_order_delivered_late_eta_max

FROM
(
SELECT     base2.source
          ,base2.source_2
          ,base2.created_date
          ,base2.report_date

          ,base2.order_status
          ,case when order_status = 'Delivered' then 1 else 0 end as is_del
          ,case when order_status = 'Cancelled' then 1 else 0 end as is_cancel

          ,base2.city_group
          ,base2.city_name
          ,base2.is_order_in_hub_shift
          ,base2.is_hub_driver
          ,case when base2.h_id > 0 and base2.is_hub_driver = 1 then 1 else 0 end as is_order_delivered_by_driver_hub
          ,case when base2.pick_h_id > 0 then 1 else 0 end as is_order_picked_at_hub
          ,base2.is_late_delivered_time_eta_max
          ,base2.is_valid_submit_to_del
          ,base2.food_service
          ,base2.is_stack_order
          ,base2.is_group_order

          ,count(distinct base2.uid) total_order

FROM
(
SELECT
         base1.uid
        ,base1.order_code
        ,case when base1.source = 'order_delivery' then 'NowFood' else 'NowShip' end as source
        ,base1.source as source_2
        ,base1.created_date
        ,base1.report_date
        ,base1.city_group
        ,base1.city_name
        ,base1.city_id
        ,base1.order_status
        ,base1.is_stack_order
        ,base1.is_group_order
        ,base1.is_late_delivered_time_eta_max
        ,base1.hub_id
        ,base1.pick_hub_id
        ,base1.h_id
        ,base1.pick_h_id
        ,base1.hub_name
        ,base1.hub_location
        ,base1.is_hub_driver
        ,base1.is_order_in_hub_shift
        ,base1.is_valid_submit_to_del
        ,base1.food_service
FROM
        (
        SELECT base.shipper_id
              ,base.city_name
              ,base.city_group
              ,base.city_id
              ,base.report_date
              ,base.created_date

              ,base.order_id
              ,base.order_code
              ,concat(base.source,'_',cast(base.order_id as varchar)) as uid
              ,base.ref_order_category order_type
              ,base.source

              ,case when base.order_status = 400 then 'Delivered'
                    when base.order_status = 401 then 'Quit'
                    when base.order_status in (402,403,404) then 'Cancelled'
                    when base.order_status in (405) then 'Returned'
                    else 'Others' end as order_status

              ,base.order_status_group
              ,base.is_stack_order
              ,base.is_group_order

              ,case when base.last_delivered_timestamp > (case when base.max_eta = 0 then base.estimated_delivered_time else from_unixtime(base.submitted_time + base.max_eta - 3600) end) then 1 else 0 end as is_late_delivered_time_eta_max
              ,case when base.created_timestamp <= base.last_delivered_timestamp then 1 else 0 end as is_valid_submit_to_del

              ,base.hub_id
              ,base.pick_hub_id
              ,coalesce(pick_hub_info.id,0) as pick_h_id
              ,coalesce(hub_info.id,0) as h_id
              ,hub_info.hub_name
              ,hub_info.hub_location
              ,base.is_hub_driver
              ,case when base.report_date between date('2021-07-09') and date('2021-10-05') and base.city_id = 217 and is_hub_driver = 1 then 1
                    when base.report_date between date('2021-07-24') and date('2021-10-04') and base.city_id = 218 and is_hub_driver = 1 then 1
                    when base.driver_payment_policy = 2 then 1 else 0 end as is_order_in_hub_shift
              ,base.food_service
        FROM
                    (
                    SELECT dot.uid as shipper_id
                          ,dot.ref_order_id as order_id
                          ,dot.ref_order_code as order_code
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

                          ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time = ogi.create_time then 1 else 0 end as is_group_order
                          ,case when dot.group_id > 0 and coalesce(group_order.order_type,0) = 0 then 1
                                when dot.group_id > 0 and coalesce(group_order.order_type,0) = 200 and ogm.create_time != ogi.create_time then 1
                                else 0 end as is_stack_order
                          ,case when dot.order_status in (400,401,405) and dot.real_drop_time > 0 then date(from_unixtime(dot.real_drop_time - 3600))
                                when dot.order_status in (402,403,404) and cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) > 0 then date(from_unixtime(cast(json_extract(dotet.order_data,'$.delivery.status_update_time') as bigint) - 3600))
                                else date(from_unixtime(dot.submitted_time- 3600)) end as report_date
                          ,date(from_unixtime(dot.submitted_time- 3600)) created_date
                          ,from_unixtime(dot.submitted_time- 3600) created_timestamp
                          ,dot.submitted_time
                          ,case when dot.real_drop_time = 0 then null else from_unixtime(dot.real_drop_time - 3600) end as last_delivered_timestamp
                          ,case when dot.estimated_drop_time = 0 then null else from_unixtime(dot.estimated_drop_time - 3600) end as estimated_delivered_time
                          ,case when dot.pick_city_id = 238 THEN 'Dien Bien' else city.name_en end as city_name
                          ,case when dot.pick_city_id = 217 then 'HCM'
                                when dot.pick_city_id = 218 then 'HN'
                                when dot.pick_city_id = 219 then 'DN'
                                ELSE 'OTH' end as city_group
                          ,dot.pick_city_id as city_id
                          ,COALESCE(cast(json_extract(dotet.order_data,'$.hub_id') as BIGINT ),0) as hub_id
                          ,COALESCE(cast(json_extract(dotet.order_data,'$.pick_hub_id') as BIGINT ),0) as pick_hub_id
                          ,case when driver_hub.shipper_type_id = 12 then 1 else 0 end as is_hub_driver
                          ,cast(json_extract(dotet.order_data,'$.shipper_policy.type') as bigint) as driver_payment_policy

                          -- eta max
                          ,eta.max_eta
                          ,case when COALESCE(oct.foody_service_id,0) = 1 then 'Food'
                                when COALESCE(oct.foody_service_id,0) in (5) then 'Fresh'
                                when COALESCE(oct.foody_service_id,0) > 0 then 'Market'
                                when dot.ref_order_category = 0 then 'Food - Others'
                                else 'Nowship' end as food_service

                    FROM (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_tab_vn_da where date(dt) = current_date - interval '1' day) dot

                    left join (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_driver_order_extra_tab_vn_da where date(dt) = current_date - interval '1' day) dotet on dot.id = dotet.order_id
                    left join shopeefood.foody_order_db__order_completed_tab__reg_continuous_s0_live oct on oct.id = dot.ref_order_id and dot.ref_order_category = 0
                    -- stack group_code
                    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm on dot.group_id = ogm.group_id and dot.ref_order_id = ogm.ref_order_id and ogm.mapping_status = 11 and ogm.ref_order_category = dot.ref_order_category

                    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_mapping_tab_vn_da where date(dt) = current_date - interval '1' day) ogm_filter on dot.group_id = ogm.group_id and dot.ref_order_id = ogm_filter.ref_order_id and ogm_filter.mapping_status = 11
                                                                                                                                        and ogm_filter.ref_order_category = dot.ref_order_category
                                                                                                                                        and ogm_filter.create_time >  ogm.create_time
                    LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id = ogm.group_id

                    left join shopeefood.foody_delivery_db__province_tab__reg_daily_s0_live city on city.id = dot.pick_city_id and city.country_id = 86

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

                    --- eta for each stage
                    LEFT JOIN
                        (SELECT
                             eta.id
                            ,eta.order_id
                            ,from_unixtime(eta.create_time - 3600) as create_time
                            ,coalesce(cast(json_extract(eta.eta_data,'$.eta_range.max') as INT),0) as max_eta

                            from shopeefood.data_mining_db__order_eta_data_tab__reg_daily_s0_live eta
                        )eta on eta.order_id =  dot.ref_order_id  -- oct.id

                    LEFT JOIN
                            (SELECT
                                    a.order_id
                                    ,a.order_type
                                    ,case when a.order_type <> 200 then order_type else ogi.ref_order_category end as order_category
                                    ,case when a.assign_type = 1 then '1. Single Assign'
                                          when a.assign_type in (2,4) then '2. Multi Assign'
                                          when a.assign_type = 3 then '3. Well-Stack Assign'
                                          when a.assign_type = 5 then '4. Free Pick'
                                          when a.assign_type = 6 then '5. Manual'
                                          when a.assign_type in (7,8) then '6. New Stack Assign'
                                          else null end as assign_type
                                    ,from_unixtime(a.create_time - 3600) as create_time
                                    ,from_unixtime(a.update_time - 3600) as update_time
                                    ,date(from_unixtime(a.create_time - 3600)) as date_
                                    ,case when cast(FROM_UNIXTIME(a.create_time - 3600) as date) between DATE('2019-12-30') and DATE('2019-12-31') then 202001
                                          when cast(FROM_UNIXTIME(a.create_time - 3600) as date) between DATE('2021-01-01') and DATE('2021-01-03') then 202053
                                            else YEAR(cast(FROM_UNIXTIME(a.create_time - 3600) as date))*100 + WEEK(cast(FROM_UNIXTIME(a.create_time - 3600) as date)) end as year_week

                            from (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                                    from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                                    where status in (3,4) -- shipper incharge

                                    UNION

                                    SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status,order_type, experiment_group

                                    from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                                    where status in (3,4) -- shipper incharge
                                )a

                                -- take last incharge
                                LEFT JOIN
                                        (SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

                                        from shopeefood.foody_partner_archive_db__order_assign_shipper_log_archive_tab__reg_daily_s0_live
                                        where status in (3,4) -- shipper incharge

                                        UNION

                                        SELECT concat(cast(order_id as VARCHAR),'-',cast(order_type as VARCHAR)) as order_uid, order_id, city_id, assign_type, update_time, create_time,status

                                        from shopeefood.foody_partner_db__order_assign_shipper_log_tab__reg_daily_s0_live
                                        where status in (3,4) -- shipper incharge
                                    )a_filter on a.order_uid = a_filter.order_uid and a.create_time < a_filter.create_time

                                -- auto accept

                               LEFT JOIN (select * from shopeefood.shopeefood_mart_dwd_foody_partner_db_order_group_info_tab_vn_da where date(dt) = current_date - interval '1' day) ogi on ogi.id > 0 and ogi.id = case when a.order_type = 200 then a.order_id else 0 end

                            where 1=1
                            and a_filter.order_id is null -- take last incharge
                            -- and a.order_id = 9490679
                            and a.order_type = 200

                            GROUP BY 1,2,3,4,5,6,7,8

                            )group_order on group_order.order_id = dot.group_id and dot.group_id > 0 and  group_order.order_category = dot.ref_order_category


                    WHERE 1=1
                    and ogm_filter.create_time is null
                    and dot.pick_city_id <> 238
                    and dot.grass_schema = 'foody_partner_db'

                    )base


        LEFT JOIN
                (
                 SELECT id, hub_name
                      ,case when city_id = 217 then 'HCM'
                            when city_id = 218 then 'HN'
                            when city_id = 219 then 'DN'
                            ELSE 'OTH' end as hub_location
                 FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live

                 WHERE 1=1
                 and id <> 2
                 and driver_count > 0
                 and grass_schema = 'foody_internal_db'

                )hub_info on hub_info.id = base.hub_id
        LEFT JOIN
                (
                 SELECT id, hub_name as pick_hub_name
                      ,case when city_id = 217 then 'HCM'
                            when city_id = 218 then 'HN'
                            when city_id = 219 then 'DN'
                            ELSE 'OTH' end as pick_hub_location
                 FROM shopeefood.foody_internal_db__shipper_hub_info_tab__reg_daily_s0_live

                 WHERE 1=1
                 and id <> 2
                 and driver_count > 0
                 and grass_schema = 'foody_internal_db'

                )pick_hub_info on pick_hub_info.id = base.pick_hub_id

        WHERE 1=1
        and base.created_date BETWEEN DATE_TRUNC('month', current_date - interval '1' day) - interval '1' month AND current_date - interval '1' day
        and base.order_status_group = 'Completed'

        )base1
)base2
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)base3
WHERE base3.source = 'NowFood'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)

SELECT
    p.period
    , p.days AS days
    --, SUM(IF(b.is_order_delivered_by_driver_hub = 1 and b.is_order_in_hub_shift = 1, b.total_order_delivered, 0)) / p.days AS hub_shift_ado
    --, SUM(IF(b.is_order_delivered_by_driver_hub = 0 or b.is_order_in_hub_shift = 0, b.total_order_delivered, 0)) / p.days AS non_hub_ado
    --, SUM(IF(b.is_order_delivered_by_driver_hub = 1 and b.is_order_in_hub_shift = 1 and b.is_stack_order = 1, b.total_order_delivered, 0)) / p.days AS hub_shift_stacked_ado
    --, SUM(IF(b.is_order_delivered_by_driver_hub = 1 and b.is_order_in_hub_shift = 1, b.total_order_delivered_late_eta_max, 0)) / p.days AS hub_shift_late_ado
    --, SUM(IF((b.is_order_delivered_by_driver_hub = 0 or b.is_order_in_hub_shift = 0) and b.is_stack_order = 1, b.total_order_delivered, 0)) / p.days AS non_hub_stacked_ado
    --, SUM(IF(b.is_order_delivered_by_driver_hub = 0 or b.is_order_in_hub_shift = 0, b.total_order_delivered_late_eta_max, 0)) / p.days AS non_hub_late_ado
    --, SUM(IF(b.is_stack_order = 1, b.total_order_delivered, 0)) as stacked_ado
    --, SUM(b.total_order_delivered_late_eta_max) / p.days AS late_ado
    , SUM(IF(b.is_stack_order = 1, b.total_order_delivered, 0)) / CAST(SUM(b.total_order_delivered) AS DOUBLE) AS stacked
    --, SUM(b.total_order_delivered_late_eta_max) / CAST(SUM(b.total_order_delivered) AS DOUBLE) AS late
    , SUM(IF(b.is_order_delivered_by_driver_hub = 1 and b.is_order_in_hub_shift = 1 and b.is_stack_order = 1, b.total_order_delivered, 0)) / CAST(SUM(IF(b.is_order_delivered_by_driver_hub = 1 and b.is_order_in_hub_shift = 1, b.total_order_delivered, 0)) AS DOUBLE) AS hub_shift_stacked
    , SUM(IF((b.is_order_delivered_by_driver_hub = 0 or b.is_order_in_hub_shift = 0) and b.is_stack_order = 1, b.total_order_delivered, 0)) / CAST(SUM(IF(b.is_order_delivered_by_driver_hub = 0 or b.is_order_in_hub_shift = 0, b.total_order_delivered, 0)) AS DOUBLE) AS non_hub_stacked
    --, SUM(IF(b.is_order_delivered_by_driver_hub = 1 and b.is_order_in_hub_shift = 1, b.total_order_delivered_late_eta_max, 0)) / CAST(SUM(IF(b.is_order_delivered_by_driver_hub = 1 and b.is_order_in_hub_shift = 1, b.total_order_delivered, 0)) AS DOUBLE) AS hub_shift_late
    --, SUM(IF(b.is_order_delivered_by_driver_hub = 0 or b.is_order_in_hub_shift = 0, b.total_order_delivered_late_eta_max, 0)) / CAST(SUM(IF(b.is_order_delivered_by_driver_hub = 0 or b.is_order_in_hub_shift = 0, b.total_order_delivered, 0)) AS DOUBLE) AS non_hub_late
    
FROM base b
INNER JOIN params p ON b.grass_date BETWEEN p.start_date AND p.end_date
GROUP BY 1,2